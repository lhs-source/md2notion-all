from notion.client import NotionClient
from notion.block import PageBlock
from md2notion.upload import upload

from glob import glob
import time
import os
import requests
import threading
import traceback

lock = threading.Lock()

def doUploadTree(rootPage):
    # key, value for tree of notion pages,
    # {'page' : 'abcdef-12345', ...}
    key_list = {
        
    }
    errorPage = []
    # root page
    key_list['./'] = rootPage

    for root, subdirs, files in os.walk('./'):
        if root == './_resources':
            continue

        # create new blanc parent pages
        for sub in subdirs:
            if sub in key_list:
                print('key exist ', sub)
            else:
                with lock:
                    newSubPage = getExistPage(key_list[root], sub)

                    if newSubPage is None:
                        newSubPage = key_list[root].children.add_new(PageBlock, title=sub)
                    else:
                        print('{} [{}] | '.format(threading.currentThread().getName(), sub), '  validation : parent exist')
                
                if root == './' and (sub != '_resources'):
                    key_list[root + sub] = newSubPage
                    print('{} [{}] | '.format(threading.currentThread().getName(), root + sub), ' create blank page ')
                else:
                    key_list[root + '\\' + sub] = newSubPage
                    print('{} [{}] | '.format(threading.currentThread().getName(), root + '\\' + sub), ' create blank page ')
        
        # create new pages and copy the markdown file
        for file in files:
            filename, file_extension = os.path.splitext(file)
            print('{} [{}] | '.format(threading.currentThread().getName(), filename), '  Create a new file : ', file)

            # critical section
            lock.acquire()
            print('{} [{}] | '.format(threading.currentThread().getName(), filename), '  the critical section has been locked')

            # check the page is already exist
            newSubPage = getExistPage(key_list[root], filename)
            if newSubPage is not None:
                print('{} [{}] | '.format(threading.currentThread().getName(), filename), '  validation : content exist')
                lock.release()
                continue
            if file_extension != '.md':
                print('{} [{}] | '.format(threading.currentThread().getName(), filename), '  validation : content is not markdown')
                lock.release()
                continue

            # go upload page
            try:
                # file open to be copied and create a new notion page
                mdFile = open(root + '\\' + file, "r", encoding="utf-8")
                newPage = key_list[root].children.add_new(PageBlock, title=filename)

                # release critical seciton
                lock.release()

                # upload file contents
                upload(mdFile, newPage)
            except:
                lock.release()
                print('{} [{}] | '.format(threading.currentThread().getName(), filename), '  Exception : ')
                errorPage.append(filename)
                traceback.print_exc()
            finally:
                continue

    print('errorPages = ', errorPage)

def getExistPage(targetPage, title):
    for page in targetPage.children:
        if page.title == title:
            return page
    return None

def uploadMd(token_v2, target, threadCnt):
    print(token_v2, target, threadCnt)
    # notion token_v2
    client = NotionClient(token_v2=token_v2)
    # target notion page
    rootPage = client.get_block(target)

    # multithread running
    threads = []
    threadCnt = 4

    for i in range(threadCnt):
        t = threading.Thread(target=doUploadTree, args=(rootPage))
        threads.append(t)

    for i in range(threadCnt):
        threads[i].start()

    for i in range(threadCnt):
        threads[i].join()
    doUploadTree(rootPage)


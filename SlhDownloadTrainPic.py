import postgre_op
import os
import urllib.request
import pandas as pd
import json
import threading
from urllib.error import URLError

Url = 'https://op.hzecool.com/slh/img.do?_cid='


def getHtml(epid, fileid, cid):
    html = urllib.request.urlopen(f'https://op.hzecool.com/slh/img.do?_cid={cid}&epid={epid}&fileId={fileid}').read()
    html = html.decode('utf-8')
    return html


def get_image(url, fileid, folder):
    try:
        if not os.path.exists(folder + '/' + fileid + '.jpg'):
            request = urllib.request.Request(url)
            response = urllib.request.urlopen(request)
            get_img = response.read()
            with open(folder + '/' + fileid + '.jpg', 'wb') as fp:
                fp.write(get_img)
                print('图片下载完成')

    except URLError:
        print('访问空')


def downloadPic():
    conn = postgre_op.get_conn()
    cur = postgre_op.get_cursor(conn)
    cur.execute("select stdclassid from calamodel.dres_with_calss_pic where stdclassid>=1300 and stdclassid>=1840 "
                "group by stdclassid having count(*)>1000 order by stdclassid")
    folders = cur.fetchall()
    for fd in folders:
        folder = 'F:/ml/dres_photos/' + str(fd[0])
        if not os.path.exists(folder):
            os.mkdir(folder)
        print(folder)
        cur.execute(
            "select t.epid, fileid, t2.cluster from calamodel.dres_with_calss_pic t inner join staging.sc_accountset "
            "t2 on t.epid=t2.id and t2.delflag=0 and t2.cluster<>'a14' where stdclassid= %d" % (fd[0]))
        pictures = cur.fetchall()
        for pic in pictures:
            epid = pic[0]
            fileid = pic[1]
            cid = pic[2]
            try:
                data = getHtml(epid, fileid, cid)
                if data.find('data') > 0 and data.find('url') > 0:
                    dj = json.loads(data)
                    url = pd.DataFrame(dj['data']['url']).loc[0].values[0]
                    if url.startswith('http'):
                        get_image(url, fileid, folder)

            except:
                print("网络连接异常")
    conn.colse()


def downloadPicEpid(tid):
    conn = postgre_op.get_conn()
    cur = postgre_op.get_cursor(conn)
    cur.execute("select epid,count(*) from calamodel.dres_with_epid_pic where epid%2 =" + str(tid - 1)
                + " group by epid having count(*)>100")
    folders = cur.fetchall()
    num = 0
    for fd in folders:
        folder = 'F:/ml/dres_photos_epid/' + str(fd[0])
        if not os.path.exists(folder):
            os.mkdir(folder)
        print(folder)
        cur.execute(
            "select t.epid, fileid, t2.cluster, to_char(marketdate,'YYYYMMdd') as marketdate from "
            "calamodel.dres_with_epid_pic t inner join staging.sc_accountset t2 on t.epid=t2.id and t2.delflag=0 "
            "where t.epid= %d" % (fd[0]))
        pictures = cur.fetchall()
        for pic in pictures:
            # if num > 10000:
            #     break
            print(folder + ':' + pic[1] + '::' + str(num))
            epid = pic[0]
            fileid = pic[1]
            cid = pic[2]
            try:
                data = getHtml(epid, fileid, cid)
                if data.find('data') > 0 and data.find('url') > 0:
                    dj = json.loads(data)
                    url = pd.DataFrame(dj['data']['url']).loc[0].values[0]
                    if url.startswith('http'):
                        get_image(url, pic[3] + '_' + fileid, folder)
                num += 1
            except:
                print("网络连接异常")
    conn.close()


class myThread(threading.Thread):
    def __init__(self, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID

    def run(self):
        print("开始线程：" + str(self.threadID))
        downloadPicEpid(self.threadID)
        print("退出线程：" + str(self.threadID))


thread1 = myThread(1)
thread2 = myThread(2)

thread1.start()
thread2.start()
thread1.join()
thread2.join()

# try:
# downloadPic()
# downloadPicEpid()
# except:
#     print
#     "Error: unable to start thread"
#     exit(-1)
exit(0)

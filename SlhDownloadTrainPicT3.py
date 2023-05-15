import postgre_op
import os
import urllib
import pandas as pd
import json
import _thread
from urllib.error import URLError

conn = postgre_op.get_conn()
cur = postgre_op.get_cursor(conn)

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
                print('图片下载完成', folder)

    except URLError:
        print('访问空')


def downloadPic2():
    cur.execute("select stdclassid from calamodel.dres_with_calss_pic where stdclassid>1090 and stdclassid < 1890"
                "group by stdclassid having count(*)>1000 order by stdclassid desc")
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

            except URLError:
                print("网络连接异常")


try:
    downloadPic2()
except:
    print("Error: unable to start thread")
    exit(-1)

while 1:
    pass

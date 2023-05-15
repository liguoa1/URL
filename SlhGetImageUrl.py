import urllib
from urllib.error import URLError

import pandas as pd
import time
import json
import postgre_op

Url = 'https://op.hzecool.com/slh/img.do?_cid='


def getHtml(epid, fileid, cid):
    html = urllib.request.urlopen(f'https://op.hzecool.com/slh/img.do?_cid={cid}&epid={epid}&fileId={fileid}').read()
    html = html.decode('utf-8')
    return html


def updateUrl():
    conn = postgre_op.get_conn()
    cur = postgre_op.get_cursor(conn)
    cur.execute("select t.epid,shopid,styleid,fileid ,t2.cluster from calcdm.pre_slh_top_dres t inner join "
                "staging.sc_accountset t2 on t.epid=t2.id and t2.delflag=0 where  url is "  # (numseq>0 or priceseq>0) and
                "null and t2.cluster<>'a14'")
    dress = cur.fetchall()
    num = 1
    Sqls = ""
    for dr in dress:
        epid = dr[0]
        shopid = dr[1]
        styleid = dr[2]
        fileid = dr[3]
        cid = dr[4]
        print(epid,fileid,cid)
        try:
            returnstr = getHtml(epid, fileid, cid)
            # print(returnstr)
            if returnstr.find('data') > 0 and returnstr.find('url') > 0:
                dj = json.loads(returnstr)
                df = pd.DataFrame(dj['data']['url'])
                Sqls = Sqls + "update calcdm.pre_slh_top_dres set url = '%s' where epid =%d and  shopid = %d and styleid = %d;" \
                       % (df.loc[0].values[0], epid, shopid, styleid)

                if num % 20 == 0:
                    cur.execute(Sqls)
                    conn.commit()
                    print(Sqls)
                    Sqls = ""
                num = num + 1
        except URLError:
            print("网络连接异常")
            num = num + 1
        if num % 40 == 0:
            time.sleep(1)

    if len(Sqls) > 0:
        cur.execute(Sqls)
        conn.commit()
        print(Sqls)


if __name__ == '__main__':
    updateUrl()

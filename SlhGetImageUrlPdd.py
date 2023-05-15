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
    cur.execute("select t.unit_id ,shop_id,style_id,fileid ,t2.cluster from calcdm.pre_pdd_shop_top30p_top50dres t "
                "inner join staging.sc_accountset t2 on t.unit_id=t2.id and t2.delflag=0 where  url is null")
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
                Sqls = Sqls + "update calcdm.pre_pdd_shop_top30p_top50dres set url = '%s' where unit_id =%d and  shop_id = %d and style_id = %d;" \
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

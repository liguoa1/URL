import DownloadImg as dli
import postgre_op as op
import threading
import queue
import platform


def getTaskQueue():
    taskQueue = queue.Queue()
    conn = op.get_conn(Host='129.211.121.162', Port='25432', DataBase='bigdm', UserName='bigdm', Password='myD1soft')
    cur = op.get_cursor(conn)
    cur.execute(
        "select  cid ,object_key from(	select t2.cid ,t.object_key,row_number() over(partition by t2.cid order by "
        "(((respond::json->>'Products')::json->>0)::json->>'Confidence')::numeric desc) seq from  "
        "image.detectproducts t inner join image.product_class t2 on (("
        "respond::json->>'Products')::json->>0)::json->>'Name'=t2.product and (("
        "respond::json->>'Products')::json->>0)::json->>'Parents' ~t2.class_name 	where  (("
        "respond::json->>'Products')::json->>0)::json is not null 	and ((("
        "respond::json->>'Products')::json->>0)::json->>'Confidence')::integer>90 )t where seq <5000 limit 20")
    imgs = cur.fetchall()
    for img in imgs:
        cid = str(img[0])
        img_key = str(img[1])
        taskQueue.put({'cid': cid, 'img_key': img_key})
    conn.close()
    return taskQueue


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while not squeue.empty():
            dt = squeue.get()
            cid = dt['cid']
            img_key = dt['img_key']
            fid = (img_key.rsplit('/', 1)[1]).split('.')[0]
            dli.load_img(img_key, cid, fid, FOLDER)


if __name__ == '__main__':
    # 多线程
    squeue = getTaskQueue()
    if 'Windows' == platform.system():
        FOLDER = 'F:/ml/detect'
    else:
        FOLDER = '/home/ml'
    for i in range(5):
        c = Consumer()  # 创建多个消费者线程
        c.start()

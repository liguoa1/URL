import os
import urllib.request
from urllib.error import URLError

BASE_URL = 'https://gdoc01a-1256054816.cos.ap-shanghai.myqcloud.com'


def load_img(img_key, cid, fid, folder):
    if not os.path.exists(folder):
        os.mkdir(folder)
    if not os.path.exists(folder + '/' + cid):
        os.mkdir(folder + '/' + cid)
    try:
        if not os.path.exists(folder + '/' + cid + '/' + fid + '.jpg'):
            print(BASE_URL + img_key)
            request = urllib.request.Request(BASE_URL + '/' + img_key)
            response = urllib.request.urlopen(request)
            get_img = response.read()
            with open(folder + '/' + cid + '/' + fid + '.jpg', 'wb') as fp:
                fp.write(get_img)
                print('图片下载完成', folder + img_key)

    except URLError:
        print('访问空')

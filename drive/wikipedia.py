
import ssl,re
from urllib import request
from bs4 import BeautifulSoup

# 设置ssl不校验证书
ssl._create_default_https_context = ssl._create_unverified_context

def hello():
    print("hello")

def get_link(url):
    req = request.Request(url)
    req.add_header('accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8')
    req.add_header('accept-language', 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7')
    req.add_header('referer', 'https://zh.wikipedia.org/wiki/%E7%AE%A1%E7%90%86%E5%BF%83%E7%90%86%E5%AD%A6')
    req.add_header('user-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36')
    resp = request.urlopen(req)
    html = resp.read()
    soup = BeautifulSoup(html,"html.parser")
    regex=re.compile(r"^(/wiki/)((?!:).)*$")
    uniq_set = set()
    ret_list = []
    for link in soup.find('div',{'id':'bodyContent'}).find_all('a', href=regex ):
        if 'href' in link.attrs:
            if not link.attrs['href'] in uniq_set:
                uniq_set.add(link.attrs['href'])
                ret_list.append((link.text, "https://zh.wikipedia.org%s" % link.attrs['href']))
    return ret_list

            
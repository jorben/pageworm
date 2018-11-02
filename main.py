import os
import sys
import time
import hashlib
import pymysql
import configparser
pymysql.install_as_MySQLdb()
import MySQLdb as db


#--------------------- 基础方法 -----------------------
WORKPATH = os.path.dirname(os.path.realpath(__file__))
CONFFILE = "%s/etc/config.ini" % WORKPATH
LOGPATH = "%s/log/" % WORKPATH

class CFrame:
    def __init__(self):
        # 打开日志文件
        if not os.path.exists(LOGPATH):
            os.makedirs(LOGPATH)
        self.logFile = "%s%s.log" % (LOGPATH, time.strftime("%Y%m%d"))
        self.plog = open(self.logFile, 'a')

        if not os.path.isfile(CONFFILE):
            self.log("error: config file(%s) is not exist" % CONFFILE)
            sys.exit(-1)
        
        self.conf = configparser.ConfigParser()
        self.conf.read(CONFFILE)

        self.db_host = ""
        self.db_user = ""
        self.db_pass = ""
        self.db_conn = None
        self.db_cursor = None
        # 存在 db 配置则 连接db
        if "db" in self.conf :
            self.db_host = self.conf.get("db", "host")
            self.db_user = self.conf.get("db", "user")
            self.db_pass = self.conf.get("db", "pass")
            if self.db_host :
                self.db_conn = db.connect(self.db_host, self.db_user, self.db_pass, "jorben")
                self.db_cursor = self.db_conn.cursor()



    def __del__(self):
        if self.db_conn:
            self.db_conn.close()
        # 关闭日志文件
        if(self.plog):
            self.plog.close()

    def md5(self, src):
        m = hashlib.md5()
        m.update(src)
        return m.hexdigest()

    def log(self, msg):
        if(self.plog):
            logMsg = "[%s] %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg.replace("\n", ""))
            self.plog.write(logMsg)

def exitProc(msg):
    # 接入告警等，如果需要
    pass
#------------------------------------------------------

#--------------------- 业务接口 -----------------------
class CApp(CFrame):
    def __init__(self, argv):
        super(CApp, self).__init__()
        # 校验参数完整性
        self.task = "fetch_test"
        if 2 <= argv.__len__() :
            self.task = argv[1]
            #print("Usage:\n%s task_no" % (argv[0]))
            #sys.exit(0)
        # 取业务配置
        self.ignore = []
        self.fetch_sleep_ms = 10
        self.min_len = 1
        if "base" in self.conf:
            self.fetch_sleep_ms = int(self.conf.get("base", "sleep_ms"))
            ignore_text = self.conf.get("base", "ignore")
            if ignore_text :
                self.ignore = ignore_text.split(",")
            if int(self.conf.get("base", "len_min")):
                self.min_len = int(self.conf.get("base", "len_min"))
        #print(self.ignore)
        
    def run(self):
        self.log("begin, task_no:%s" % self.task)
        # 取任务 入口URL
        sql = "SELECT Ftype,Furl,Fdepth FROM t_task WHERE Ftask_no='%s'" % self.task
        self.log('sql:%s' % sql)
        self.db_cursor.execute(sql)
        result = self.db_cursor.fetchone()
        if not result :
            self.log("task:%s is not exists." % self.task)
            return 
        self.drive = __import__("drive.%s" % (result[0]), fromlist=(result[0]) )
        self.top_level = result[2]
        # 获取多级索引接口
        # self.fetch_index(result[1], result[2])
        self.drive.hello()
        
    def fetch_index(self, index_url, depth=2, pid=0):
        '''
        多层级获取链接网络
        '''
        if 0 < self.fetch_sleep_ms:
            time.sleep(self.fetch_sleep_ms / 1000)
            self.log("usleep %d ms" % self.fetch_sleep_ms)
        ret = self.drive.get_link(index_url)
        later = []
        # 拿到链接地址 写入库表
        if ret :
            for url in ret:
                if self.__fidder(url[0]):
                    continue
                new_pid = self.ins_index(self.task, pid, self.top_level-depth+1, url[0], url[1])
                # 先把本轮的信息入库，避免儿子变孙子
                if (new_pid and 1 < depth ) :
                    later.append((url[1], depth-1, new_pid))
            self.log("pid:%d level:%d finish list size %d" % (pid, self.top_level-depth+1, ret.__len__()))
        # 递归处理子节点
        if later :
            for son in later:
                self.fetch_index(son[0], son[1], son[2])

    def __fidder(self, text):
        '''
        过滤检查，ret 0 有效内容, 非0 无效内容需过滤
        '''
        # 太短
        if self.min_len > len(text):
            return 1 
        # 已指定的忽略词
        if text in self.ignore:
            return 1
        # 词中包含有· 常用于人名 比如 伊利雅胡·高德拉特, 奥利弗·威廉姆森
        if not (-1 == text.find("·") and -1 == text.find(".")):
            return 1
        # 某某大学 某某学院
        if (("大学" == text[-2:]) or ("学院" == text[-2:])):
            return 1

        return 0

    def clear_index(self, id, top=0):
        '''
        根据id清理链接，含子链接
        '''
        sql = 'SELECT Fid FROM t_index where Fpid = %d' % id
        self.log('sql:%s' % sql)
        self.db_cursor.execute(sql)
        result = self.db_cursor.fetchall()
        # 根据PID 批量删除
        if result:
            # 递归子节点
            for row in result:
                self.clear_index(row[0])
            sql = 'DELETE FROM t_index WHERE Fpid = %d' % id
            self.log('sql:%s' % sql)
            self.db_cursor.execute(sql)
            self.db_conn.commit()
        # 是否顶端节点 删除自身
        if top:
            sql = 'DELETE FROM t_index WHERE Fid = %d' % id
            self.log('sql:%s' % sql)
            self.db_cursor.execute(sql)
            self.db_conn.commit()



    def ins_index(self, task_no, pid, level, title, url):
        '''
        索引入库
        '''
        md5 = self.md5(url.encode("utf8"))
        sql = """INSERT INTO t_index(
            Ftask_no, Fcode, Fpid, Flevel, Ftitle, Furl, Fstatus, Fcreate_time
            ) VALUES(
            '%s', '%s', %d, %d, '%s', '%s', %d, '%s'
            )""" % (task_no, md5, pid, level, title, url, 0, time.strftime("%Y-%m-%d %H:%M:%S"))
        ins_id = 0
        try:
            self.db_cursor.execute(sql)
            ins_id = self.db_conn.insert_id()
            self.db_conn.commit()
        except db.Error as e:
            self.log("ERROR: %s, SQL:%s" % (e, sql))
            #raise
        return ins_id

#------------------------------------------------------

if __name__ == '__main__':
    app = CApp(sys.argv)
    app.run()
    # 清理一些不需要的 词
    # app.clear_index(14, 1)
    '''
    try:
        app = CApp(sys.argv)
        app.run()
    except SystemExit as e :
        pass
    except :
        msg = sys.exc_info()[0]
        exitProc("[%s] 程序异常退出，请介入检查！\n[error: %s]" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))
        print("error: %s" % msg)
        #sys.exit(-1)
    #sys.exit(0)
    '''
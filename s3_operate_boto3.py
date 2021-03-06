#!/usr/bin/env python
# coding=utf-8

#@date 2018-03-28
#@author zhenyu.zhu

import boto3
import botocore

import sys
import os
import time
import ConfigParser as rwini


def readConfig(inifile):
    ini = rwini.ConfigParser()
    ini.read(inifile)
    return ini


def __strformat__(fmt, *arg):
    return fmt % arg


#返回相应参数，没有则返回 ""
#param:ini 文件名 section key
#全部按照str类型读取
def getConfig(ini, section, key):
    try:
        value = ini.get(section, key)
        return value
    except Exception as e:
        #print "[ERR] %s sec:%s key:%s" % (e.message, section, key)
        return ""


#获取连接句柄
def getResource(access, secret):
    region_name = 'cn-north-1'

    return boto3.resource('s3', 
        aws_access_key_id = access, 
        aws_secret_access_key = secret, 
        region_name = region_name
    )


#当没该bucket的时候跑出异常
#return bucket以及bucket list
def getBucket(resource, bucketname):
    bucket = resource.Bucket(bucketname)
    exists = True
    try:
        conn.meta.client.head_bucket(Bucket=bucketname)
        return exists, bucket
    except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
        return exists, None


#return 指定prefix的key
def __bucketObjfilter_prefix__(bucket, prefix, **kwargs):
    return bucket.objects.filter(
        Prefix=prefix,
        kwargs
    )


def __object_exsit__(bucket, key):
    

def downloadObj(bucket, key, filename, **kwargs):
    bucket.download_file(key, filename, kwargs)

##没有filter，表示list所有符合prefix的文件,TODO:根据正则匹配文件名
#param conn链接 bucketname bucket名 path本地存储位置 prefix文件前缀 hfilter文件过滤器
def downloadBucket(bucketname, path="", prefix="", hfilter=None):
    if conn is None:
        print r"no connection avaliable, param 'conn' can't be None"
        return
    filenum = 0
    #获取所有bucket下的key
    try:
        exists, bucket = __getBucket__(conn, bucketname)
        if exists is not True:
            return  

        if path != "" and not path.endswith("/"):
                path = path + "/"

        objlist = __bucketObjfilter__(bucket, list)
        for key in bucketlist:
            filename = path + key.name
            fsplit = filename.split("/")
            fpath = "/".join(fsplit[:-1])
            #不存在需要创建文件夹，否则会失败。
            if not os.path.exists(fpath):
                os.makedirs(fpath)

            print "[INFO]: downloading", fsplit[-1]
            key.get_contents_to_filename(filename)
            filenum = filenum + 1
    except Exception as e:
        print "[ERR]:", e.message
    finally:
        return filenum


#上传文件，两种模式 --文件夹模式即上传文件夹下所有文件，有递归和非递归两种
#                 --文件模式即上传单个文件
#filepath表示需要上传文件的路径
#上传之后的文件路径为    bucket/fdir/dirname/filename (filename为单个文件名，不带文件夹,dirname为上一级目录名)
#RETURN  a(上传的文件数量), b(跳过的文件数量)
def uploadBucket(conn, bucketname, filepath, fdir, mode = "dir", overwrite = "0", recursive = None):
    if conn is None:
        print r"no connection avaliable, param 'conn' can't be None"
        return
    filenum = 0
    fileskipnum = 0
    #uploadsize = 0
    try:
        bucket, _ = __getBucketlist__(conn, bucketname)
        if filepath.endswith("/"):
            filepath = filepath[:-1]
        #date = time.strftime("%Y-%m-%d", time.localtime())
        
        if mode == "dir" and os.path.isdir(filepath):
            #上传文件夹下所有文件(非递归)
            filelist = os.listdir(filepath)
            
            for f in filelist:
                rfile = filepath + "/" + f
                #跳过文件夹和.开头的隐藏文件
                if not os.path.isfile(rfile) or f.startswith("."):
                    continue

                rfilepath = filepath.split("/")[-1]
                keyname = "{fdir}/{dirname}/{filename}".format(
                    fdir = fdir,
                    #date = date,
                    dirname = rfilepath,
                    filename = f
                    )
                #增加开关控制是否跳过同名文件，还是覆盖
                if overwrite != "1" and bucket.get_key(keyname) is not None:
                    print keyname, "exists skip"
                    fileskipnum = fileskipnum + 1
                    continue
                k = Key(bucket)
                k.name = keyname
                
                print "[INFO]: uploading", f
                k.set_contents_from_filename(rfile)
                filenum = filenum + 1
        elif mode == "file" and os.path.isfile(filepath):
            #上传单个文件
            
            #rfile = filepath.split("/")
            #if rfile.startswith("/"):
            #    rfile = rfile[1:]
            dirname = filepath.split("/")[-2]
            fname = filepath.split("/")[-1]

            keyname = "{fdir}/{dirname}/{filename}".format(
                    fdir = fdir,
                    dirname = dirname,
                    filename = fname
                    )
            #增加开关控制是否跳过同名文件，还是覆盖
            if overwrite != "1" and bucket.get_key(keyname) is not None:
                print keyname, "exists skip"
                fileskipnum = fileskipnum + 1
                continue

            k = Key(bucket)
            k.name = keyname
            print "[INFO]: uploading", fname
            k.set_contents_from_filename(filepath)
            filenum = filenum + 1
    except Exception as e:
        print "[ERR]:uploadBucket", e.message
    finally:
        return filenum, fileskipnum


def download(conn, ini):
    downloadnum = 0
    bucket  = getConfig(ini, "download", "bucket")
    if bucket == "":
        bucket = "speech-datacollection"

    to_path = getConfig(ini, "download", "to_path")
    ddir    = getConfig(ini, "download", "dir")
    if ddir == "":
        ddir = "asr-voice"

    usr_prefix = getConfig(ini, "download", "usr_prefix")
    date    = getConfig(ini, "download", "date") #TODO日期各种格式适配
    if date == "":
        date = time.strftime("%Y-%m-%d", time.localtime())

    #bucket中搜索文件的前缀，如果没有给出usr_prefix，就用 dir/date 的形式作为prefix
    #否则用用户自定义的usr_prefix
    prefix = ""
    if usr_prefix != "":
        prefix = usr_prefix
    else:
        prefix = "{}/{}".format(ddir, date)

    prefixlist = prefix.split(";")
    for p in prefixlist:
        if p == "":
            continue
        downloadnum = downloadnum + downloadBucket(conn, bucket, prefix = p, path = to_path)
    print "[INFO]: download finished"
    print "[INFO]: download", downloadnum, "files"


def upload(conn, ini):
    uploadnum = 0
    skipnum = 0
    bucket = getConfig(ini, "upload", "bucket")
    if bucket == "":
        bucket = "speech-datacollection"

    fileordir = getConfig(ini, "upload", "fileordir")
    mode   = getConfig(ini, "upload", "mode")
    if mode == "":
        mode == "dir"
    #bucket下一级目录名称
    firstdir = getConfig(ini, "upload", "dirname")
    if firstdir == "":
        firstdir = "resource"

    overwrite = getConfig(ini, "upload", "overwrite")
    if overwrite != "1":
        overwrite = "0"
    
    fordlist = fileordir.split(";")
    for f in fordlist:
        if f == "":
            continue
        u, s = uploadBucket(conn, bucket, f, firstdir, overwrite = overwrite, mode = mode)
        uploadnum = uploadnum + u
        skipnum = skipnum + s
    print "[INFO]: upload finished"
    print "[INFO]: upload", uploadnum, "files, skip", skipnum, "files"


def usage():
    print r"[usage]: python %s (download|upload) conf.ini" % sys.argv[0]
    exit(0)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()

    if sys.argv[1] == "-h" or sys.argv[1] == "--help":
        usage()

    if not os.path.isfile(sys.argv[2]):
        print sys.argv[2], "is not ini file"
        exit(0)

    ini = readConfig(sys.argv[2])
    access_key = getConfig(ini, "global", "access_key")
    secret_key = getConfig(ini, "global", "secret_key")
    host       = getConfig(ini, "global", "host")

    conn = getConnection(access_key, secret_key, host)

    opr = sys.argv[1]
    if opr == "download":
        #下载
        download(conn, ini)
    elif opr == "upload":
        #上传
        upload(conn, ini)
    else:
        print r"err opr: %s" % sys.argv[1]

    conn.close()
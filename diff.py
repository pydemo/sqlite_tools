import os, sys, time
from os.path import join, isfile, isdir, dirname, basename
import sqlite3
from subprocess import Popen, PIPE
from pprint import pprint as pp
import subprocess
from pathlib import Path
from include.utils import dict2
from collections import OrderedDict
import xlsxwriter

con = sqlite3.connect("recon.db")
e=sys.exit


def drop_table(tname):
    cmd=['DROP TABLE %s;' % tname.strip()]
    exec_sqlite(cmd)

def exec_sqlite(incmd):
    spath = Path('sqlite3.exe').resolve()
    dpath = Path('recon.db').resolve()
    csv_file = Path('%s_ocean.csv' % REPORT).resolve()
    cmd=[spath, dpath] + incmd
    #pp(cmd)	

    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell = True)
    retout=[]
    errout = []
    while p.poll() == None: 

        out=p.stdout.readline()
        if out:
            #print('OUTPUT:', out)
            retout.append(out)
        er=p.stderr.readline()
        if er:
            print('ERROR:', er)
            errout.append(er)
    p.wait()

    rcode = p.returncode
    #print('RETCODE: ', rcode)
    return  rcode, retout, errout

def load_table(tname, fname):
    assert isfile(fname), fname
    cmd=[ '.mode csv', ".separator ','", '.import %s. %s' % (fname, tname), 'SELECT count(*) from %s;' % tname]

    return exec_sqlite(cmd)


def perr(errors):
    if errors:
        print('#' *80)
        print('#' *80)
        for er in errors:
            print('ERROR: ', er)
        print('#' *80)
        print('#' *80)
def pdata(data):
    if data:
        for out in data:
            print('-' *80)
            print('| OUTPUT: ', out.decode().strip())
            print('-' *80)

def exec_report(rloc):
    global con
    rpath = Path(rloc).resolve()
    print(rpath)
    with open(rpath, 'r') as fh:
        repq = fh.read()
    #print(repq)

    cur = con.cursor()
    cur.execute(repq)
    #pp(cur.description)
    return cur

def create_xls(data):
    global TRADE_DATE, OUT_FILE
    
    fn =OUT_FILE
    floc = fn
    #fpath = Path(fn).resolve(floc)
    
    with xlsxwriter.Workbook(floc) as workbook:
        bold = workbook.add_format({'bold': True})
        for i, (k, row) in enumerate(data.items(), start=0):
            title = '%s(%s)' % (k, len(row.data))
            worksheet = workbook.add_worksheet(title)
            

        
            # Write headers
            
            worksheet.write(0, 0, 'REPORT', bold)
            worksheet.write(0, 1, 'TRADE_DATE', bold)
            
            for i, col in enumerate(row.header):
                worksheet.write(0, i+2, col[0], bold)
            #worksheet.write(0, 1, 'Quanitity')

            # Write dict data
            for i, val in enumerate(row.data, start=1):
                worksheet.write(i, 0, REPORT)
                worksheet.write(i, 1, TRADE_DATE)
                for j, v in enumerate(val):
                    worksheet.write(i, j+2, v)
                    
            for i, col in enumerate([0,0]+list(row.header)):
                worksheet.set_column(0, i, 25)
    if 0:
        import webbrowser
        webbrowser.open(floc)
    print('XLS saved to', floc)
def create_count_distinct(rloc):
        cols={}
        for tname in TABLES.values():
            cols[tname] = []
        for tname in TABLES.values():
            cur = con.cursor()
            cur.execute('SELECT * FROM %s' % tname)
            for col in cur.description:
                cols[tname].append(col[0])
        #pp(cols)
        #e()
        from_tab, to_tab = TABLES.left, TABLES.right

        out=[]
        for cid, col in enumerate(cols[from_tab]):
            out.append("""
    select 'COUNT DISTINCT' Report, '{from_column}' Column, Ocean, Olympus, Ocean-Olympus diff  
    FROM (SELECT (select count(distinct "{from_column}")  from {from_table}) Ocean,
    (select count(distinct "{to_column}")  from {to_table}) Olympus ) tt 
    """.format(**dict(from_column=col, to_column=cols[to_tab][cid], from_table=from_tab, to_table=to_tab))
            )
            
        
        rsql = 'SELECT * FROM  ('+'UNION ALL'.join(out) + ') WHERE diff!=0;'
        with open(rloc, 'w') as fh:
            fh.write(rsql)

def create_not_in(rloc):
        cols={}
        for tname in TABLES.values():
            cols[tname] = []
        for tname in TABLES.values():
            cur = con.cursor()
            cur.execute('SELECT * FROM %s' % tname)
            for col in cur.description:
                cols[tname].append(col[0])
        #pp(cols)
        from_tab, to_tab = TABLES.left, TABLES.right
        out=[]
        for cid, col in enumerate(cols[from_tab]):
            out.append("""
SELECT 'NOT IN' Report, '{from_column}' Column, (select count(*) 
FROM {to_table} 
WHERE "{from_column}" NOT IN (select "{to_column}" FROM {from_table})) {to_table},
(select count(*) FROM {from_table} WHERE "{from_column}" NOT IN (select "{to_column}" FROM {to_table})) {from_table}
    """.format(**dict(from_column=col, to_column=cols[to_tab][cid], from_table=from_tab, to_table=to_tab))
            )
            
        
        rsql = """SELECT Report, Column,{from_table}||'('||(from_cnt-{from_table})||')' {from_table},{to_table}||'('||(to_cnt-{to_table})||')'  {to_table}  FROM  (""".format(**dict(from_table=from_tab, to_table=to_tab)) + \
        'UNION ALL'.join(out) + \
        '), (SELECT count(*) from_cnt from {from_table}) from_cnt, (SELECT count(*) to_cnt from {to_table}) to_cnt '.format(**dict(from_table=from_tab, to_table=to_tab))  + \
        """ WHERE {from_table}+{to_table}>0""".format(**dict(from_table=from_tab, to_table=to_tab))
        with open(rloc, 'w') as fh:
            fh.write(rsql)			
def create_count_diff(rloc):
        global TABLES
        cols={}
        rdir=dirname(rloc)
        if not isdir(rdir):
            os.makedirs(rdir)
        for tname in TABLES.values():
            cols[tname] = []
        for tname in TABLES.values():
            cur = con.cursor()
            cur.execute('SELECT * FROM %s' % tname)
            for col in cur.description:
                cols[tname].append(col[0])
        #pp(cols)
        from_tab, to_tab = TABLES.left, TABLES.right
        out=[]
        #for cid, col in enumerate(cols[from_tab]):
        out.append("""
    select 'FAILURE' Result,  Ocean Ocean_Count, Olympus Olympus_Count, Ocean-Olympus Total_Diffs  
    FROM (SELECT (select count(*)  from {from_table}) Ocean,(select count(*)  from {to_table}) Olympus ) tt 
    """.format(**dict(from_table=from_tab, to_table=to_tab))
            )
        if 0:
            out.append("""
        select 'COUNT DISTINCT DIFF' Report,  Ocean, Olympus, Ocean-Olympus diff  
        FROM (SELECT (select count(*)  from 
        (SELECT DISTINCT a.* FROM {from_table} a) ) Ocean,(select count(*)  from (SELECT DISTINCT b.* FROM {to_table} b)) Olympus ) tt 
        """.format(**dict(from_table=from_tab, to_table=to_tab))
                )
            
        rsql = 'SELECT * FROM  ('+'UNION ALL'.join(out) + ') WHERE Total_Diffs!=0;'
        with open(rloc, 'w') as fh:
            fh.write(rsql)

def create_in(rloc):
        cols={}
        for tname in TABLES.values():
            cols[tname] = []
        for tname in TABLES.values():
            cur = con.cursor()
            cur.execute('SELECT * FROM %s' % tname)
            for col in cur.description:
                cols[tname].append(col[0])
        #pp(cols)
        from_tab, to_tab = TABLES.left, TABLES.right
        out=[]
        for cid, col in enumerate(cols[from_tab]):
            out.append("""
SELECT 'IN' Report, '{from_column}' Column, (select count(*) 
FROM {to_table} 
WHERE "{from_column}" IN (select "{to_column}" FROM {from_table})) {to_table},
(select count(*) FROM {from_table} WHERE "{from_column}" IN (select "{to_column}" FROM {to_table})) {from_table}
    """.format(**dict(from_column=col, to_column=cols[to_tab][cid], from_table=from_tab, to_table=to_tab))
            )
            
        
        rsql = """SELECT Report, Column,{from_table}||'('||({from_table}-from_cnt)||')' {from_table},{to_table}||'('||({to_table}-to_cnt)||')'  {to_table}  FROM  (""".format(**dict(from_table=from_tab, to_table=to_tab)) + \
        'UNION ALL'.join(out) + \
        '), (SELECT count(*) from_cnt from {from_table}) from_cnt, (SELECT count(*) to_cnt from {to_table}) to_cnt '.format(**dict(from_table=from_tab, to_table=to_tab))  + \
        """ WHERE {from_table}+{to_table}>0""".format(**dict(from_table=from_tab, to_table=to_tab))
        with open(rloc, 'w') as fh:
            fh.write(rsql)			
def create_in_distinct(rloc):
        cols={}
        for tname in TABLES.values():
            cols[tname] = []
        for tname in TABLES.values():
            cur = con.cursor()
            cur.execute('SELECT * FROM %s' % tname)
            for col in cur.description:
                cols[tname].append(col[0])
        #pp(cols)
        from_tab, to_tab = TABLES.left, TABLES.right
        out=[]
        for cid, col in enumerate(cols[from_tab]):
            out.append("""
SELECT 'IN DISTINCT' Report, '{from_column}' Column, (SELECT count(*) FROM (select DISTINCT "{to_column}"
FROM {to_table} 
WHERE "{from_column}" IN (select DISTINCT "{to_column}" FROM {from_table}))) {to_table},
(SELECT count(*) FROM (select DISTINCT "{from_column}" FROM {from_table} 
WHERE "{from_column}" IN (select DISTINCT "{to_column}" FROM {to_table}))) {from_table},
(SELECT count(*) from_cnt FROM (SELECT DISTINCT "{from_column}" from_cnt from {from_table})) from_cnt,
(SELECT COUNT(*) to_cnt FROM (SELECT DISTINCT "{to_column}" to_cnt from {to_table})) to_cnt
    """.format(**dict(from_column=col, to_column=cols[to_tab][cid], from_table=from_tab, to_table=to_tab))
            )
            
        
        rsql = """SELECT Report, Column,{from_table}||'('||({from_table}-from_cnt)||')' {from_table},{to_table}||'('||({to_table}-to_cnt)||')'  {to_table}  FROM  (""".format(**dict(from_table=from_tab, to_table=to_tab)) + \
        'UNION ALL'.join(out) + \
        """)
         """.format(**dict(from_column=col, to_column=cols[to_tab][cid], from_table=from_tab, to_table=to_tab))  + \
        """ WHERE {from_table}+{to_table}>0""".format(**dict(from_table=from_tab, to_table=to_tab))
        with open(rloc, 'w') as fh:
            fh.write(rsql)			
def create_not_in_distinct(rloc):
        cols={}
        for tname in TABLES.values():
            cols[tname] = []
        for tname in TABLES.values():
            cur = con.cursor()
            cur.execute('SELECT * FROM %s' % tname)
            for col in cur.description:
                cols[tname].append(col[0])
        #pp(cols)
        from_tab, to_tab = TABLES.left, TABLES.right
        out=[]
        for cid, col in enumerate(cols[from_tab]):
            out.append("""
SELECT 'NOT IN DISTINCT' Report, '{from_column}' Column, (SELECT count(*) FROM (select DISTINCT "{to_column}" 
FROM {to_table} 
WHERE "{from_column}" NOT IN (select DISTINCT "{to_column}" FROM {from_table}))) {to_table},
(SELECT count(*) FROM (select DISTINCT "{from_column}" FROM {from_table} 
WHERE "{from_column}" NOT IN (select DISTINCT "{to_column}" FROM {to_table}))) {from_table},
(SELECT count(*) from_cnt FROM (SELECT DISTINCT "{from_column}" from_cnt from {from_table})) from_cnt,
(SELECT COUNT(*) to_cnt FROM (SELECT DISTINCT "{to_column}" to_cnt from {to_table})) to_cnt
    """.format(**dict(from_column=col, to_column=cols[to_tab][cid], from_table=from_tab, to_table=to_tab))
            )
            
        
        rsql = """SELECT Report, Column,{from_table}||'('||(from_cnt-{from_table})||')' {from_table},
        {to_table}||'('||(to_cnt-{to_table})||')'  {to_table}  FROM  (""".format(**dict(from_table=from_tab, to_table=to_tab)) + \
        'UNION ALL'.join(out) + \
        """)
         """.format(**dict(from_column=col, to_column=cols[to_tab][cid], from_table=from_tab, to_table=to_tab))  + \
        """ WHERE {from_table}+{to_table}>0""".format(**dict(from_table=from_tab, to_table=to_tab))
        with open(rloc, 'w') as fh:
            fh.write(rsql)			
import click
click.disable_unicode_literals_warning = True

REPORT = 'mexico'
TABLES = dict2(left='%s_ocean' % REPORT, right= '%s_olympus' % REPORT)

REPORTS_DIR = join ('reports', REPORT)

def find_files(files, dirs=[], extensions=[]):
    # https://stackoverflow.com/a/45646357/2441026

    new_dirs = []
    for d in dirs:
        try:
            new_dirs += [ os.path.join(d, f) for f in os.listdir(d) ]
            #pp(new_dirs)
        except OSError:
            if os.path.splitext(d)[1].lower() in extensions:
                files.append(d)

    if new_dirs:
        find_files(files, new_dirs, extensions )
    else:
        return
        
def run_fast_scandir(dir, ext):    # dir: str, ext: list
    # https://stackoverflow.com/a/59803793/2441026

    subfolders, files = [], []

    for f in os.scandir(dir):
        if f.is_dir():
            subfolders.append(f.path)
        if f.is_file():
            print (os.path.splitext(f.name)[1].lower())
            if os.path.splitext(f.name)[1].lower() in ext:
                files.append(f.path)


    for dir in list(subfolders):
        sf, f = run_fast_scandir(dir, ext)
        subfolders.extend(sf)
        files.extend(f)
    return subfolders, files
#// python 2diff.py -l C:\tmp\dump\AMC\NZ.AMC_RECON_ocean.sql.2020-06-29.0.csv -r C:\tmp\dump\AMC\AMC_olympus_ALL.csv -o report.xlsx -td 2020-06-29
@click.command()

@click.option('-l', '--left',   default = None, 	help = 'Ocean CSV dir.', 	required=True )
@click.option('-r', '--right',  default = None,	    help = 'Olympus CSV dir.', required=True )
@click.option('-o', '--out_dir', default = None,	help = 'Out xls dir',  	required=True )
#@click.option('-td', '--trade_date', default = None,	help = 'Trade_date',  	required=True )
def load_data(**kwargs):
    """
    python.bat 3diff.py -l C:/tmp\dump\5days\mexico\Ocean\2020-08-19\ -r C:/tmp\dump\5days\mexico\Olympus\20200819\ -o C:\tmp\dump\5days\report
    python.bat 3diff.py -l C:\tmp\dump\5days\data\OPRA\Ocean -r C:\tmp\dump\5days\data\OPRA\Olympus -o C:\tmp\dump\5days\report\OPRA
    
    """
    OUT_DIR = join(kwargs['out_dir'], REPORT)
    if isdir(OUT_DIR): 
        import shutil
        shutil.rmtree(OUT_DIR)
        #os.removedirs()
    DIRS=dict2(left=kwargs['left'], right=kwargs['right'])
    #pp(DIRS)
    assert isdir(DIRS.left), DIRS.left
    assert isdir(DIRS.right), DIRS.right
    from os import listdir
    
    _, files=run_fast_scandir(DIRS.left, ['.csv'])
    left= {basename(f.replace('-','')):dict2(src=f, base=basename(f)) for f in [f for f in files if isfile(join(DIRS.left, f))]}
    #pp(left)
    #e()
    for k,v in left.items():
        v.trdt=k.split('.')[0].split('_')[-1]
        v.mrkt=k.split('.')[0].split('_')[0]
        v.query=k.split('.')[0].split('_')[1]
        v.report=REPORT
    _, files=run_fast_scandir(DIRS.right, ['.csv'])
    
    right= {basename(f.replace('-','')):dict2(src=f, base=basename(f)) for f in [f for f in files if isfile(join(DIRS.right, f))]}
    for k,v in right.items():
        v.trdt=k.split('.')[0].split('_')[-1]
        v.mrkt=k.split('.')[0].split('_')[0]
        v.query=k.split('.')[0].split('_')[1]
        v.report=REPORT

    #pp(left)
    #pp(right)
    for fn in left:
        
        assert fn in right, fn
    for k, v in left.items():
        kwargs['left']=join(DIRS.left,v.src)
        kwargs['right']=join(DIRS.right,right[k].src)
        odir = join(OUT_DIR,v.trdt)
        if not isdir(odir): os.makedirs(odir) 
        kwargs['out_file']=join(odir,"{report}_{mrkt}_{query}_{trdt}_diff.xlsx".format(**v))
        kwargs['trade_date']=v.trdt
        kwargs['market']=v.mrkt
        kwargs['query']=v.query
        #pp(kwargs)
        #e()
        load_file(**kwargs)
        #pp(gcount)
        #e()
    create_diffs_xls('total_diffs.xlsx' , gcount)
    e()
    files=[]
    for ff in files:
        kwargs.update(ff)
        load_file(**kwargs)
    e()
    
gcount={}
def load_file(**kwargs):
    global FILES, TRADE_DATE, OUT_FILE, TABLES
    OUT_FILE=kwargs['out_file']
    TRADE_DATE = kwargs['trade_date']
    FILES=dict2(left=kwargs['left'].replace('\\','/'), right=kwargs['right'].replace('\\','/'))
    MARKET=kwargs['market']
    QUERY=kwargs['query']
    #pp(FILES)
    #pp(TABLES)
    #e()
    TABLES = dict2(left='ocean_{market}_{query}_{trade_date}'.format(**kwargs)  , right= 'olympus_{market}_{query}_{trade_date}'.format(**kwargs))
    for side, tname in TABLES.items():
        print('Dropping: ' , tname)
        drop_table(tname)
        print('Loading: ' , tname)
        status, data, errors = load_table(tname, FILES[side])
        pdata(data)
        perr(errors)
        
    if 1:
        xls = dict2()
        if 1:
            
            rloc = join(REPORTS_DIR, 'count_diff.sql')
            create_count_diff(rloc)
            cur=exec_report(rloc)
            data=cur.fetchall()
            header=cur.description
            if not data: 
                data=[('SUCCESS', 0, 0, 0)]
            xls.count_diff = dict2(**{'data' :data, 'header':header})
            out_bn = basename(OUT_FILE)
            print(out_bn)
            
            gcount[out_bn] = dict2(diff=dict2(count_diff=xls.count_diff), trdt=TRADE_DATE, details=out_bn, mrkt=MARKET, query=QUERY)
           # e()
        if 1:
            
            rloc = join(REPORTS_DIR, 'count_distinct.sql')
            create_count_distinct(rloc)
            cur=exec_report(rloc)
            xls.count_distinct = dict2(**{'data' :cur.fetchall(), 'header':cur.description})
            #e()
        if 1:
            rloc = join(REPORTS_DIR, 'not_in.sql')
            create_not_in(rloc)
            cur=exec_report(rloc)
            xls.not_in = dict2(**{'data' :cur.fetchall(), 'header':cur.description})
        if 1:
            rloc = join(REPORTS_DIR, 'in.sql')
            create_in(rloc)
            cur=exec_report(rloc)
            xls._in = dict2(**{'data' :cur.fetchall(), 'header':cur.description})
            
        if 1:
            rloc = join(REPORTS_DIR, 'in_distinct.sql')
            create_in_distinct(rloc)
            cur=exec_report(rloc)
            xls._in_distinct = dict2(**{'data' :cur.fetchall(), 'header':cur.description})
        if 1:
            rloc = join(REPORTS_DIR, 'not_in_distinct.sql')
            create_not_in_distinct(rloc)
            cur=exec_report(rloc)
            xls.not_in_distinct = dict2(**{'data' :cur.fetchall(), 'header':cur.description})
            

        
        create_xls(xls)
    
        
def create_diffs_xls(fn, all_data):
    global TRADE_DATE, OUT_FILE
    
    
    floc = fn
    #fpath = Path(fn).resolve(floc)
    
    with xlsxwriter.Workbook(floc) as workbook:
        bold = workbook.add_format({'bold': True})
        worksheet=None
        rid=0
        for k, v  in all_data.items():
            data=v.diff
            pp(data)
            
            for i, (k, row) in enumerate(data.items(), start=0):
                title = '%s(%s)' % (k, len(row.data))
                if not worksheet:
                    worksheet = workbook.add_worksheet(title)
                

                
                    # Write headers
                    
                    worksheet.write(0, 0, 'TRADE_DATE', bold)
                    #worksheet.write(0, 1, 'Report', bold)
                    worksheet.write(0, 1, 'Market', bold)
                    worksheet.write(0, 2, 'Query', bold)
                
                for i, col in enumerate(list(row.header)+[['Diffs_file']]):
                    worksheet.write(0, i+3, col[0], bold)

                # Write dict data
                for i, val in enumerate(row.data, start=1):
                    worksheet.write(i+rid, 0, v.trdt)
                    #worksheet.write(i+rid, 1, REPORT)
                    worksheet.write(i+rid, 1, v.mrkt)
                    worksheet.write(i+rid, 2, v.query)
                    for j, vv in enumerate(list(val)+[v.details]):
                        worksheet.write(i+rid, j+3, vv)
                    
                for i, col in enumerate([0,0]+list(row.header)):
                    worksheet.set_column(0, i+rid, 20)
                rid +=1
    if 1:
        import webbrowser
        webbrowser.open(floc)
    print('Total Diffs XLS saved to', floc)
if __name__ == "__main__":
    if 1:
        load_data()
        
        #e()

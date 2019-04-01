#!/bin/sh
HOST=10.101.14.16
USER=ftprbs
PASSWORD=ftpc

ftp -inv $HOST <<EOF
user $USER $PASSWORD
prompt
quote rcmd sbmjob cmd(call ftp004c parm('CRU 20181 SALES UPO')) INLLIBL(*JOBD) job(SALES20181) log(4 0 *seclvl) jobd(MMCCLCTL/JDAJOBD)
quit
EOF
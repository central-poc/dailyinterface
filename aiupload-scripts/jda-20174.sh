#!/bin/sh
HOST=10.101.14.16
USER=ftprbs
PASSWORD=ftpc

ftp -inv $HOST <<EOF
user $USER $PASSWORD
prompt
quote rcmd sbmjob cmd(call ftp004c parm('CDS 20174 SALES UPO')) INLLIBL(*JOBD) job(SALES20174) log(4 0 *seclvl) jobd(MMCCLCTL/JDAJOBD)
quit
EOF

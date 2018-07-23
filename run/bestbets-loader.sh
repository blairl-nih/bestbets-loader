#!/bin/bash
export LOG_FILE=run/bestbets-loader.log

if [[ $THIS_HOST_NAME == ncias-p* ]]
then 
    export NOTIFICATION_EMAIL_RECIPIENTS=$NCIALERT_EMAIL
else 
    export NOTIFICATION_EMAIL_RECIPIENTS=$DEV_EMAIL
fi

export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion

function logger
{
  if [ $# -gt 0 ]; then
    echo "[$(date +"%Y%m%d-%H:%M:%S") $THIS_SCRIPT_NAME] $@" >> $LOG_FILE
    echo "[$(date +"%Y%m%d-%H:%M:%S") $THIS_SCRIPT_NAME] $@"
  else
    while read data
    do
      echo "[$(date +"%Y%m%d-%H:%M:%S") $THIS_SCRIPT_NAME] $data" >> $LOG_FILE
      echo "[$(date +"%Y%m%d-%H:%M:%S") $THIS_SCRIPT_NAME] $data"
    done
  fi
}

function exit_on_error
{
  EXIT_LINE_NUMBER=$1
  ERROR_MESSAGE=$2

  email_notification "exited after error at line number $EXIT_LINE_NUMBER"  "ERROR: $ERROR_MESSAGE"

  logger $ERROR_MESSAGE
  logger "ERROR: Exiting $THIS_SCRIPT_NAME after error with exit code 1 at line number: $EXIT_LINE_NUMBER. Goodbye."

  exit 1
}


function email_notification
{
  SUBJECT=$1
  MESSAGE=$2

  SUBJECT_FINAL="$THIS_SCRIPT_NAME on $THIS_HOST_NAME: $SUBJECT"
  MESSAGE_FINAL="$THIS_SCRIPT_NAME on $THIS_HOST_NAME at $(date +"%Y%m%d-%H:%M:%S"): $MESSAGE"

  #echo "................................. EMAIL: $SUBJECT_FINAL: $MESSAGE_FINAL"
  ( echo $MESSAGE_FINAL ) | /bin/mailx -s "$SUBJECT_FINAL" $NOTIFICATION_EMAIL_RECIPIENTS

  if (( $? )) ; then
    logger "ERROR: email_notification: mailx exited with non-zero status code $? while trying to send: Subject: $SUBJECT_FINAL, Message: $MESSAGE_FINAL"
  fi
}





nvm use 8
cd /home/nutch/bestbets-loader
node index.js


exit_code=$?


if (( $exit_code )) ; then
  logger "ERROR: bestbets-loader exited with non-zero status code $exit_code"
  exit_on_error $LINENO "ERROR: bestbets exited with non-zero status code $exit_code"
fi


logger "INFO: bestbets loader : End"

exit 0

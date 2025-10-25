python3 -m pip install kubernetes pystache
FOLDER=$(echo "$GIT_REPOSITORY" | awk -F'/' '{print $NF}' | cut -d'.' -f1)
nohup python3 ./work/$FOLDER/init-cluster.py > /home/onyxia/work/initialisation.log 2>&1 &

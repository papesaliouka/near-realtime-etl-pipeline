if you use the cloud database:
    you can install the dependencies and run the dev command.
else:
    make sure you clone the cloud db first by using the dump script and second the restore script.
    and before dumping the db run the mongo daemon with this command
        sudo mongod -f /etc/mongod.conf
    to run the database localy should do this steps:
    1. edit the /etc/mongod.conf file use your favorite text editor and add this line to the config file
    if you need more infos read this stackoverflow link https://stackoverflow.com/questions/59571945/the-changestream-stage-is-only-supported-on-replica-sets-error-while-using-mo
    << replication:
        replSetName: "rs0" >>
    2. run the mongod daemon with sudo privilege
        sudo mongod -f /etc/mongod.conf
    3. run the mongo shell and initiate the replication of the db
        a- run mongosh
        and run in the mongo shell type:
        b-rs.initiate()
        finaly exit the shell
        c- ctrl c or type exit()
    4 run npm run dev 

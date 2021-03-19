if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
cd tables || { echo "Cannot cd into tables/"; exit 1; }
echo "10000\n5\n1\n"| java QueryMain ../queries/inq/selectJoinCidLGTCartid.sql ../testoutput/inq/selectJoinCidLGTCartid.txt
cd .. || { echo "Error cd out of tables/"; exit 1; }
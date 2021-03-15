if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
cd tables || { echo "Cannot cd into tables/"; exit 1; }
echo "10000\n10\n1\n"| java QueryMain ../queries/testDistinct_fromTable.sql ../testoutput/testDistinct_fromTable.out
echo "10000\n10\n1\n"| java QueryMain ../queries/testDistinct_fromJoin.sql ../testoutput/testDistinct_fromJoin.out
echo "10000\n10\n1\n"| java QueryMain ../queries/testDistinct_starFromTable.sql ../testoutput/testDistinct_starFromTable.out
echo "10000\n10\n1\n"| java QueryMain ../queries/testDistinct_starFromJoin.sql ../testoutput/testDistinct_starFromJoin.out
cd .. || { echo "Error cd out of tables/"; exit 1; }
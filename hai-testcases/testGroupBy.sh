if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
cd tables || { echo "Cannot cd into tables/"; exit 1; }
echo "400\n50\n1\n"| java QueryMain ../queries/testGroupBy_fromTable.sql ../testoutput/testGroupByFromTable.out
echo "400\n50\n1\n"| java QueryMain ../queries/testGroupBy_selectStarFromTable.sql ../testoutput/testGroupBy_selectStarFromTable.out
cd .. || { echo "Error cd out of tables/"; exit 1; }
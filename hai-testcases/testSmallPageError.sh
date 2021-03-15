if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
cd tables || { echo "Cannot cd into tables/"; exit 1; }
echo "95\n1\n"| java QueryMain ../queries/query1.sql ../testoutput/query1_smallpage.out # test with small page
echo "160\n10\n1\n"| java QueryMain ../queries/query3.sql ../testoutput/query3_smallpage.out # test with small page
cd .. || { echo "Error cd out of tables/"; exit 1; }
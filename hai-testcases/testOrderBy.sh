if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
cd tables || { echo "Cannot cd into tables/"; exit 1; }
echo "400\n50\n1\n"| java QueryMain ../queries/query_orderby.sql ../testoutput/out_orderby.out
echo "400\n5\n1\n"| java QueryMain ../queries/query_orderby2.sql ../testoutput/out_orderby2.out
cd .. || { echo "Error cd out of tables/"; exit 1; }
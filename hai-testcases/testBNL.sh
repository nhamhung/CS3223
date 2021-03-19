if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
cd tables || { echo "Cannot cd into tables/"; exit 1; }
#echo "10000\n5\n1\n"| java QueryMain ../queries/testCrossProduct.sql ../testoutput/testCrossProduct.out # To fix cross product
#echo "10000\n5\n1\n"| java QueryMain ../queries/bnlquery.sql ../testoutput/bnlquery.out # generic 133 values
echo "10000\n10\n1\n"| java QueryMain ../queries/bnlquery3Tables.sql ../testoutput/bnlquery3Tables.out # generic 272 values
#echo "200\n3\n1\n"| java QueryMain ../queries/query3.sql ../testoutput/out3_3buffer.out # expect this to take a long time
#echo "2000000\n3\n1\n"| java QueryMain ../queries/query3.sql ../testoutput/out3_bigbuffer.out # expect this to have 500 values
#echo "200\n50\n1\n"| java QueryMain ../queries/query3.sql ../testoutput/out3_500buffer.out # expect this to have 500 values
#echo "200\n50\n1\n"| java QueryMain ../queries/query31.sql ../testoutput/out31.out # expect this to be empty
#echo "200\n50\n1\n"| java QueryMain ../queries/query32.sql ../testoutput/out32.out # expect this to be empty
#echo "200\n50\n1\n"| java QueryMain ../queries/query33.sql ../testoutput/out33.out # expect this to be empty
cd .. || { echo "Error cd out of tables/"; exit 1; }
if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
echo "95\n1\n"| java QueryMain queries/query1.sql testoutput/query1.out # test with small page
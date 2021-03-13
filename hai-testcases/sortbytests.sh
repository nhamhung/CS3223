if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi

echo "400\n50\n1\n"| java QueryMain queries/query_orderby.sql testoutput/out_orderby.out
echo "400\n5\n1\n"| java QueryMain queries/query_orderby2.sql testoutput/out_orderby2.out
if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi

echo "400\n10\n1\n"| java QueryMain queries/query_orderby.sql testoutput/out_orderby.out
echo "400\n5\n1\n"| java QueryMain queries/query_orderby2.sql testoutput/out_orderby2.out
echo "400\n1\n"| java QueryMain queries/query_orderby3.sql testoutput/out_orderby3.out
echo "400\n1\n"| java QueryMain queries/query_orderby_empty.sql testoutput/out_orderby_empty.out

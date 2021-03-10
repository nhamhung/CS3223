if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi

echo "500\n200\n1\n"| java QueryMain queries/query_orderby.sql testoutput/out_orderby.out
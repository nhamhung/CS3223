if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi

echo "200000\n100\n1\n"| java QueryMain queries/query_orderby.sql testoutput/out_orderby.out
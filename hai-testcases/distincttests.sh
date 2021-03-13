if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi

echo "400\n50\n1\n"| java QueryMain queries/query_distinct.sql testoutput/out_distinct.out
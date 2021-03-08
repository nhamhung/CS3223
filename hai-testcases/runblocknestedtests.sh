if [ ! -d "./testoutput" ]
then
    mkdir testoutput
fi
echo "200\n3\n1\n"| java QueryMain query3.sql testoutput/out3_3buffer.out # expect this to take a long time
echo "200\n50\n1\n"| java QueryMain query3.sql testoutput/out3_500buffer.out # expect this to have 500 values
echo "200\n50\n1\n"| java QueryMain query31.sql testoutput/out31.out # expect this to be empty
echo "200\n50\n1\n"| java QueryMain query32.sql testoutput/out32.out # expect this to be empty
echo "200\n50\n1\n"| java QueryMain query33.sql testoutput/out33.out # expect this to be empty
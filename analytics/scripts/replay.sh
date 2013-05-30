dt=$1
ydt=$2
hr=$3
dt_str=$4

./process.sh $dt $ydt $hr $dt_str

./sqoop.sh
start=$(date +%s)
ls -al
ls -al
ls -al
sleep 3s
end=$(date +%s)

python -c "print('temps exec : ' + str(${end} - ${start}))" > ./logs/dump.txt



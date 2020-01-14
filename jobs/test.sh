start=$(date +%s.%N)
ls -al
ls -al
ls -al
end=$(date +%s.%N)

python -c "print('temps exec : ' + str(${end} - ${start}))"



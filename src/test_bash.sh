#!/bin/bash

NUM_CPU=$(nproc)
OUTFILE="output_test.txt"

DIR="./TestFiles/Input"     # cartella con i file
key="chiave"
mode="0"

# Svuota il file di output
> "$OUTFILE"

# Itera su tutti i file della directory
for file in "$DIR"/*; do
   
    word=$(basename "$file")

    for i in {1..3}; do
        {
            echo "=============================="
            echo " ITERAZIONE $i - tester_MPI - file: $word"
            echo "=============================="
        } | tee -a "$OUTFILE"

        mpirun --oversubscribe -np  "$NUM_CPU" python3 ./tester_mpi.py "$word" "$key" "$mode" 2>&1 | tee -a "$OUTFILE"

        echo "" | tee -a "$OUTFILE"
    done
    echo "" | tee -a "$OUTFILE"
    for i in {1..3}; do
        {
            echo "=============================="
            echo " ITERAZIONE $i - tester - file: $word"
            echo "=============================="
        } | tee -a "$OUTFILE"

        python3 ./tester.py "$word" "$key" "$mode" 2>&1 | tee -a "$OUTFILE"

        echo "" | tee -a "$OUTFILE"
        sleep 1
    done
    echo "" | tee -a "$OUTFILE"  
    
    for i in {1..3}; do
       {
            echo "=============================="
            echo " ITERAZIONE $i - tester_bzip - file: $word"
            echo "=============================="
        } | tee -a "$OUTFILE"

        python3 ./tester_bzip.py "$word" 2>&1 | tee -a "$OUTFILE"
        echo "" | tee -a "$OUTFILE"
        sleep 1
    done
    echo "" | tee -a "$OUTFILE"   

done
echo "Test completati. Risultati salvati in $OUTFILE" | tee -a "$OUTFILE"

#! /bin/sh -x
for BENCH in 'satellite' 'rover' 'smartphone' 'umtranslog' 'zenotravel' 'miconic'
do
	python bin/benchmarking.py $BENCH -N 5 -T 10 --savefig "${BENCH}-N5-T10.pdf" 2> /dev/null
done
for BENCH in 'p-satellite' 'p-rover' 'p-smartphone' 'p-umtranslog' 'p-zenotravel'
do
	python bin/benchmarking.py $BENCH -N 5 -T 10 --savefig "${BENCH}-N5-T10.pdf" 2> /dev/null
done
for BENCH in 'p-satellite' 'p-rover' 'p-smartphone' 'p-umtranslog' 'p-zenotravel'
do
	python bin/benchmarking.py $BENCH -N 10 -T 60 --savefig "${BENCH}-N10-T60.pdf" 2> /dev/null
done

Bootstrap: docker
From: ubuntu

%files
    ## We copy the files belonging to the planner to a dedicated folder in the image.
	. /planner

%post

    ## The "%post"-part of this script is called after the container has
    ## been created with the "%setup"-part above and runs "inside the
    ## container". Most importantly, it is used to install dependencies
    ## and build the planner. Add all commands that have to be executed
    ## once before the planner runs in this part of the script.

    ## Install all necessary dependencies.
    apt-get update
    apt-get -y install python3
    apt-get -y install python3-pip

    ## go to directory and make the planner
    python3 -m pip install antlr4-python3-runtime
    python3 -m pip install jinja2
    python3 -m pip install networkx
    cd /planner
    git clone https://gitlab.com/oara-architecture/planning/pddl-python
    cd pddl-python
    python3 setup.py install
    cd /planner


%runscript
    ## The runscript is called whenever the container is used to solve
    ## an instance.

    DOMAINFILE=$1
    PROBLEMFILE=$2
    PLANFILE=$3

    #stdbuf -o0 -e0 /planner/planner $DOMAINFILE $PROBLEMFILE 2>&1 | tee $PLANFILE
    PYTHONPATH=. python3 bin/shop.py -d -H --htn --filter-static --tdg-filter-useless -I $DOMAINFILE $PROBLEMFILE > $PLANFILE


## Update the following fields with meta data about your submission.
## Please use the same field names and use only one line for each value.
%labels
Name        HiPOP
Description Hierarchical Partial-Order Planning
Authors     Charles Lesire <charles.lesire@onera.fr> Alexandre Albore <alexandre.albore@onera.fr>
SupportsRecursion yes
SupportsPartialOrder no

conda env create -f environment.yml
conda init
source activate my_env
python -m ipykernel install --user --name my_env --display-name "Python (my_env)"
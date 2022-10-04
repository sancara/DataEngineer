# DataEngineer

## Create virtualenv
- open terminal
- pip install virtualenv
- nav to directory
- python3 -m venv "venv name"
- source "venv name"/bin/activate

The console user will now be:
  ("venv name")laptopname $

For getting out the virtual environment, just type deactivate

## Install packages by requirements.txt
pip install -r requirements.txt

"venv name"/bin/jupyter lab -> runs the jupyter notebooks (sometimes --no-browser is required)

## Create requirements.txt
1- source "venv name"/bin/activate
2- pip freeze > requirements.txt

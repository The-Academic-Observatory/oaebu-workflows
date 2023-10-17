# Compiling Documentation with Sphinx

## Windows
1) Install Python 3.10 (not from the Windows store, make sure to check `add to environment variables`): https://www.python.org/downloads/release/python-3810/
2) Disable Windows App Installer for Python: https://superuser.com/questions/1437590/typing-python-on-windows-10-version-1903-command-prompt-opens-microsoft-stor
3) In a Windows Command Prompt (not powershell), open the `oaebu-workflows\docs` folder. 
3) Create virtual env: `C:\Users\MyUser\AppData\Local\Programs\Python\Python38\python.exe -m venv venv`. Make sure to replace `MyUser` with the name of your user.
4) Activate virtual env: `.\venv\Scripts\activate`
5) Add docs directory directory to Python path: `set PYTHONPATH=%PYTHONPATH%;<pathtorepository>\oaebu-workflows\docs`. Make sure to replace `pathtorepository` with the file path to the `oaebu-workflows` folder
6) Install documentation dependencies: `pip install -r requirements.txt`
7) Build documentation: `make html`
8) To view the documentation, open `_build\html\index.html`.

## MacOS
1) Install Python 3.10 
3) In the terminal, navigate to the `oaebu-workflows/docs` folder. 
3) Create virtual env, if using conda: `conda create -n <nameofvenv> python=3.10 anaconda`
4) Activate virtual env: `conda activate <nameofvenv>`
5) Add docs directory directory to Python path: `PYTHONPATH="${PYTHONPATH}:<pathtorepository>/oaebu-workflows/docs"` followed by `export PYTHONPATH`. Make sure to replace `pathtorepository` with the file path to the `oaebu-workflows` folder
6) Install documentation dependencies: `pip install -r requirements.txt`
7) Build documentation: `make html` or `make latexpdf`
8) To view the documentation, open `_build/html/index.html` or `_build/latex/index.html`.

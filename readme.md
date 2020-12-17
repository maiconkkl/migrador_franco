# Migrador Franco

Escopo desse repositorio
-----------------
 
Esse repositorio foi criado com intuito de facilitar a migrações do banco G6 para outro sistema. 

Como instalar
-----------------
* Ele foi desenvolvido para o python 3.8, não quer dizer que não funcione com futuras versões do python ou até mesmo que 
funcione com versões anteriores. Então esse fica sendo um dos primeiro requisitos.

* Depois de instalar o python fica sendo necessario criar um ambiente virtual para controle as bibliotecas utlizadas
dentro desse projeto. O mesmo pode ser criado usando o seguinte comando pelo CMD:


    python -m venv venv
    
> Lembrando que o python precisa esta nas variaveis ambientes do windows para que o comando acima seja reconhecido pelo CMD.

* Depois precisamos ativar o ambiente virtual que acabamos de criar usando o comando

    
    venv\Scripts\activate.bat
    
> É necessario que a pasta ativa do CMD seja a pasta que acabamos de criar o ambiente virtual, caso não seja use o 
> comando CD para se deslocar até a pasta correta


* Agora vamos instalar as bibliotecas utilizadas no projeto com o comando abaixo:


    pip install -r requeriments.txt
    
Quando todos os passos acima forem concluidos a instalação esta finalizada.

O que vai ser migrado ?
-----------------
* **Tributação federal - Já esta completo**
* **Tributação estadual - Já esta completo**
* **Unidade de medida - Já esta completo**
* **Grupo - Já esta completo**
* **Subgrupo - Já esta completo**
* **Laboratorio - Não tem na digisat**
* **Produtos - Já esta completo**
* **Pessoas - Já esta completo**
* Plano de contas
* Contas
* **Pagamentos - Não precisa migrar dito por Rick**

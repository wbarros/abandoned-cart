# abandoned-cart

# Como executar este projeto
	- Use o gerenciador de pacote maven (https://maven.apache.org/) para realizar o build.
		- mvn clean package install
		- mvn exec:java -Dexec.mainClass=com.b2w.cart.AbandonedCart -Pdirect-runner -Dexec.args="--runner=DirectRunner"
		
# Estrutura do projeto
	- src/ --> implementação das classes.
	- input/ --> onde se deve colocar o arquivo de entrada json.
	- output/ --> a resposta saira neste local em formato json.
	
# Tecnologias utilizadas neste projeto
	- Java 8
	- Apache Beam
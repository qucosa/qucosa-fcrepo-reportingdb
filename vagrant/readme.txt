Installation mit Vagrant


- Installation von VirtualBox auf dem Host System
- Installation Vagrant auf dem Host System
- Installation von Ansible auf dem Host System

- ausführen eines mvn clean install -DskipITs über die Konsole, oder Projekt Entwickler IDE

- war File aus dem Projekt target Verzeichnis kompieren und in das vagrant/ansible/files Verzeichnis kopieren

- auf der Konsole in das vagrant Verzeichnis des Projektes wechseln und folgenden Befehl aus führen -> vagrant up --provision


Auf der VM werden Java7, Tomcat7, Postgresql 9.5 installiert. 
Ebenfalls wird ein automatisches Deployment der ReportingDB Applikation ausgeführt.

Die ReportingDB Applikation wird nicht als Servlet, sondern als Listener auf Basis von Threads ausgeführt.

Für den lokalen Testbetrieb, ohne vagrant / ansible Installation muss die Datei default.properties in das Verzeichnis
/var/local/ hinein kopiert und dort angepasst werden.
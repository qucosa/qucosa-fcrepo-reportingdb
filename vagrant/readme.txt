Install reportingDB project

If you use the project local, then follow local installation, otherwise follow install with ansible only.


Local installation.

	- Install or update virtual box by the latest exists version on your host system.
	- Install or upadate vagrant by the latest exists version on your host system.
	- Install or update ansible by the latest exists version on your host system.


Install with ansible only.










- ausführen eines mvn clean install -DskipITs über die Konsole, oder Projekt Entwickler IDE

- war File aus dem Projekt target Verzeichnis kompieren und in das vagrant/ansible/files Verzeichnis kopieren

- auf der Konsole in das vagrant Verzeichnis des Projektes wechseln und folgenden Befehl aus führen -> vagrant up --provision


Auf der VM werden Java7, Tomcat7, Postgresql 9.5 installiert. 
Ebenfalls wird ein automatisches Deployment der ReportingDB Applikation ausgeführt.

Die ReportingDB Applikation wird nicht als Servlet, sondern als Listener auf Basis von Threads ausgeführt.

Für den lokalen Testbetrieb, ohne vagrant / ansible Installation muss die Datei default.properties in das Verzeichnis
/var/local/ hinein kopiert und dort angepasst werden.
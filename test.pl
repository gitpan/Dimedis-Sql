use strict;
use Test;

BEGIN { plan tests => 7 }

ok ( module_loaded("Dimedis::Sql") );
ok ( module_loaded("Dimedis::SqlDriver::mysql") );
ok ( module_loaded("Dimedis::SqlDriver::Oracle") );
ok ( module_loaded("Dimedis::SqlDriver::Informix") );
ok ( module_loaded("Dimedis::SqlDriver::Sybase") );
ok ( module_loaded("Dimedis::SqlDriver::Pg") );
ok ( module_loaded("Dimedis::SqlDriver::ASAny") );
ok ( module_loaded("Dimedis::SqlDriver::ODBC") );

print "\n";
print "Das Programm dsql_test.pl f�hrt intensive Tests durch,\n";
print "braucht daf�r aber Schreibzugriff auf eine Datenbank.\n";
print "Einfach dsql_test.pl f�r die Usage eingeben.\n\n";

sub module_loaded {
	my ($module) = @_;
	printf ("Loading module %-35s ", $module." ... ");
	eval "use $module";
	return $@ ? 0 : 1;
}

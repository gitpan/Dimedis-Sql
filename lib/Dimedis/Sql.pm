package Dimedis::Sql;

use strict;
use vars qw($VERSION);
use Carp;

$VERSION = '0.30';

my $exc = "Dimedis::Sql:";	# Exception-Type prefix

my %known_data_types = (	# bekannte Datentypen
	'serial'  => 1,
	'date'    => 1,
	'clob'    => 1,
	'blob'    => 1,
	'varchar' => 1,
	'char'    => 1,
	'integer' => 1,
	'numeric' => 1,
);

my %known_operators = (		# bekannte Operatoren
	'='	 => 1,
	'!='	 => 1,
	'like'	 => 1
);

sub get_dbh			{ shift->{dbh}				}
sub get_debug			{ shift->{debug}			}
sub get_type			{ shift->{type}				}
sub get_cache			{ shift->{cache}			}
sub get_serial_write		{ shift->{serial_write}			}
sub get_utf8			{ shift->{utf8}				}

sub set_debug			{ shift->{debug}		= $_[1]	}
sub set_type			{ shift->{type}			= $_[1]	}
sub set_cache			{ shift->{cache}		= $_[1]	}
sub set_serial_write		{ shift->{serial_write}		= $_[1]	}
sub set_utf8			{ shift->{utf8}			= $_[1]	}

# Konstruktor --------------------------------------------------------

sub new {
	my $class = shift;
	my %par = @_;
	my  ($dbh, $debug, $type, $cache, $serial_write, $utf8) =
	@par{'dbh','debug','type','cache','serial_write','utf8'};

	$type ||= {};
	
	# Abw�rtskompatibilit�t: wenn cache nicht angegeben ist,
	# wird das Caching eingeschaltet.

	if ( not exists $par{cache} ) {
		$cache = 1;
	}

	# Parametercheck
	
	croak "$exc:new\tmissing dbh" if not $dbh;

	# Datenbanktyp ermitteln

	my $db_type = $dbh->{Driver}->{Name};

	# Sonderbehandlung fuer das Proxymodul	
	if ( $db_type eq "Proxy") {
	  # Aus dem DSN die eigentlichen Datenbanktyp ermitteln
	  $dbh->{Name} =~ m/;dsn=dbi:([^:]+):/;
	  $db_type     = $1;
	}

	# Instanzhash zusammenbauen
	
	my $self = {
		dbh          => $dbh,
		debug        => $debug,
		db_type      => $db_type,
		db_features  => undef,
		type_href    => $type,
		cache        => $cache,
		serial_write => $serial_write,
		utf8         => $utf8,
	};

	$debug && print STDERR "$exc:new\tdb_type=$db_type\n";

	# datenbankspezifische Methoden definieren
	require "Dimedis/SqlDriver/$db_type.pm";

	bless $self, "Dimedis::SqlDriver::$db_type";
	
	$self->{db_features} = $self->db_get_features;

	# ggf. Encode Modul laden
	require Encode if $utf8;
	
	return $self;
}

# Datentyp-Check -----------------------------------------------------

sub check_data_types {
	my $self = shift;
	
	my ($type_href, $data_href, $action) = @_;

	my $serial_found;
	my $blob_found;
	
	my ($col, $type);
	while ( ($col,$type) = each %{$type_href} ) {
	
		# Nur der Datentyp ohne Groessenangabe
		$type =~ s/\([^\(]+\)$//;
		
		croak "$exc:check_data_types\ttype $type unknown"
			unless defined $known_data_types{$type};

		if ( $type eq 'serial' ) {
			# Serials d�rfen nur 1x vorkommen
			if ( exists $data_href->{$col} ) {
				croak "$exc:check_data_types\tmultiple serial type"
					if $serial_found;
				$serial_found = $col;
			}
			# wurde was anderes als undef �bergeben,
			# dann Exception
			croak "$exc:check_data_types\t".
			    "only the undef value allowed for serial columns"
			    	if defined $data_href->{$col} and
				   not $self->{serial_write};
			
		} elsif ( $type eq 'date') {
			# GROBER Datumsformatcheck
			croak "$exc:check_data_types\t".
			    "illegal date: $col=$data_href->{$col}"
				if $data_href->{$col} and
				   $data_href->{$col} !~
				   /^\d\d\d\d\d\d\d\d\d\d:\d\d:\d\d$/;
		} elsif ( $type eq 'blob' or $type eq 'clob' ) {
			$blob_found = 1 if exists $data_href->{$col};
		}
	}

	croak "$exc:check_data_types\tblob/clob handling only with serial column"
		if $action eq 'insert' and $blob_found and
		   (not $serial_found or
		    not exists $data_href->{$serial_found});

	return $serial_found;
}

# INSERT -------------------------------------------------------------

sub insert {
	my $self = shift;
	my %par = @_;

	$par{type} ||= $self->{type_href}->{$par{table}}; # wenn undef, globales Type Hash holen

	# Parametercheck
	
	croak "$exc:insert\tmissing table" unless defined $par{table};
	croak "$exc:insert\tmissing data"  unless defined $par{data};

	$self->check_data_types (
		$par{type}, $par{data}, 'insert'
	);

	# Hier kein UTF8 Upgrading, wird beim sp�teren
	# $self->do ( sql => ... ) gemacht. Die Werte
	# in Data sind noch nicht unbedingt die finalen
	# Werte (z.B. bei Blobs k�nnen hier Filenamen
	# drin stehen, die an dieser Stelle also noch
	# nicht zu UTF8 gewandelt werden d�rfen).

	# Driver-Methode aufrufen
	my $serial;
	eval {
		$serial = $self->db_insert (\%par);
	};
	croak "$exc:insert\t$@" if $@;

	return $serial;	
}

# UPDATE -------------------------------------------------------------

sub update {
	my $self = shift;
	my %par = @_;
	
	$par{type}   ||= $self->{type_href}->{$par{table}}; # wenn undef, globales Type Hash holen
	$par{params} ||= [];	# wenn undef, leeres Listref draus machen
	
	# Parametercheck
	
	croak "$exc:insert\tmissing table" unless defined $par{table};
	croak "$exc:insert\tmissing data"  unless defined $par{data};
	croak "$exc:insert\tmissing where" unless defined $par{where};

	my $serial_found = $self->check_data_types (
		$par{type}, $par{data}, 'update'
	);
	
	croak "$exc:insert\tserial in update not allowed" if $serial_found;
	
	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} ) {
		foreach my $p ( $par{where}, @{$par{params}} ) {
			utf8::upgrade($p);
		}
	}
	
	# Kein UTF8 Upgrading auf %{$data}, wird beim sp�teren
	# $self->do ( sql => ... ) gemacht. Die Werte
	# in %{$data} sind noch nicht unbedingt die finalen
	# Werte (z.B. bei Blobs k�nnen hier Filenamen
	# drin stehen, die an dieser Stelle also noch
	# nicht zu UTF8 gewandelt werden d�rfen).

	# Driver-Methode aufrufen
	
	my $modified;
	eval {
		$modified = $self->db_update (\%par);
	};
	croak "$exc:update\t$@" if $@;

	return $modified;
}

# BLOB_READ ----------------------------------------------------------

sub blob_read {
	my $self = shift;
	my %par = @_;
	
	$par{params} ||= [];	# wenn undef, leeres Listref draus machen

	# Parametercheck
	
	croak "$exc:blob_read\tmissing table" unless defined $par{table};
	croak "$exc:blob_read\tmissing where" unless defined $par{where};
	croak "$exc:blob_read\tmissing col"   unless defined $par{col};
	croak "$exc:blob_read\tgot filehandle and filename parameter"
		if defined $par{filehandle} and defined $par{filename};
                
	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} ) {
		foreach my $p ( $par{where}, @{$par{params}} ) {
			utf8::upgrade($p);
		}
	}
	
	# Driver-Methode aufrufen
	my $blob;
	eval {
		$blob = $self->db_blob_read (\%par);
	};

	croak "$exc:blob_read\t$@" if $@;

	# ggf. UTF8 Flag setzen, wenn clob
	if ( $blob and $self->{utf8} and
	     $self->{type_href}->{$par{table}}->{$par{col}} eq 'clob' ) {
	        $self->{debug} && print STDERR "$exc:db_blob_read: Encode::_utf8_on\n";
		Encode::_utf8_on($$blob);
	}

	return $blob;
}

# DO -----------------------------------------------------------------

sub do {
        my $self = shift;

	my %par = @_;
	
        my $sql       = $par{sql};
	my $par_cache = $par{cache};
	my $no_utf8   = $par{no_utf8};
	my $params    = $par{params};

	$params ||= [];
	
	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} and not $no_utf8 ) {
		foreach my $p ( $par{sql}, @{$params} ) {
			utf8::upgrade($p);
		}
	}
	
	# Normalerweise werden SQL Statements hier von DBI gecached.
	# Es gibt aber Befehle, bei denen das keinen Sinn macht.
	# Deshalb gibt es drei Mechanismen, die das Caching steuern:
	
	# 1. wenn keine SQL Parameter �bergeben wurden, gehen wir davon
	#    aus, da� das Statement die Parameter enth�lt. In diesem
	#    Fall wollen wir das Statement nicht cachen.
	my $use_prepare_cached = @{$params};

	# 2. �ber den Parameter cache kann das Caching explizit
	#    gesteuert werden
	if ( exists $par{cache} ) {
		$use_prepare_cached = $par_cache;
	}

	# 3. wenn das Caching beim Erzeugen des Dimedis::Sql Instanz
	#    abgeschaltet wurde, gibt's kein Caching!
	
	$use_prepare_cached = 0 if not $self->{cache};

        $self->{debug} && print STDERR "$exc:do: sql = $sql\n";
        $self->{debug} && print STDERR "$exc:do: params = ".
		join(",", @{$params}), "\n";

	my $sth;
	if ( $use_prepare_cached ) {
		$self->{debug} && print STDERR "$exc:do: statement is cached\n";
		$sth = $self->{dbh}->prepare_cached ($sql);
	} else {
		$self->{debug} && print STDERR "$exc:do: statement is NOT cached\n";
		$sth = $self->{dbh}->prepare ($sql);
	}
	croak "$exc:do\t$DBI::errstr\n$sql" if $DBI::errstr;

	for ( @{$params} ) { $_ = undef if $_ eq '' };

	my $modified = $sth->execute (@{$params});
	croak "$exc:do\t$DBI::errstr\n$sql" if $DBI::errstr;
	
	$sth->finish;
	
	return $modified;
}

sub do_without_cache {
        my $self = shift;
	my %par  = @_;

        my $sql    = $par{sql};
	my $params = $par{params} ||= [];
	
	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} ) {
		foreach my $p ( $par{sql}, @{$params} ) {
			utf8::upgrade($p);
		}
	}
	
        $self->{debug} && print STDERR "$exc:do: sql = $sql\n";
        $self->{debug} && print STDERR "$exc:do: params = ".
		join(",", @{$params}), "\n";

        my $modified = $self->{dbh}->do ($sql, undef, @{$params});
	
	croak "$exc:do\t$DBI::errstr\n$sql" if $DBI::errstr;
	
	return $modified;
}

# GET ----------------------------------------------------------------

sub get {
        my $self = shift;

 	my %par = @_;

        my $sql       = $par{sql};
	my $par_cache = $par{cache};
	my $params    = $par{params};

	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} ) {
		foreach my $p ( $par{sql}, @{$params} ) {
			utf8::upgrade($p);
		}
	}
	
        my $dbh = $self->{dbh};

	# Normalerweise werden SQL Statements hier von DBI gecached.
	# Es gibt aber Befehle, bei denen das keinen Sinn macht.
	# Deshalb gibt es drei Mechanismen, die das Caching steuern:
	
	# 1. wenn keine SQL Parameter �bergeben wurden, gehen wir davon
	#    aus, da� das Statement die Parameter enth�lt. In diesem
	#    Fall wollen wir das Statement nicht cachen.
	my $use_prepare_cached = defined $params;

	# 2. �ber den Parameter cache kann das Caching explizit
	#    gesteuert werden
	if ( exists $par{cache} ) {
		$use_prepare_cached = $par_cache;
	}

	# 3. wenn das Caching beim Erzeugen des Dimedis::Sql Instanz
	#    abgeschaltet wurde, gibt's kein Caching!
	
	$use_prepare_cached = 0 if not $self->{cache};

        $self->{debug} && print STDERR "$exc:get sql = $sql\n";

        my $sth;
	
	if ( $use_prepare_cached ) {
		$self->{debug} && print STDERR "$exc:get: statement is cached\n";
		$sth = $dbh->prepare_cached ($sql)
			or croak "$exc:get\t$DBI::errstr\n$sql";
	} else {
		$self->{debug} && print STDERR "$exc:get: statement is NOT cached\n";
		$sth = $dbh->prepare ($sql)
			or croak "$exc:get\t$DBI::errstr\n$sql";
	}

        $sth->execute (@{$params})
		or croak "$exc:get\t$DBI::errstr\n$sql";

        if ( wantarray ) {
		my $lref = $sth->fetchrow_arrayref;
		# ggf. UTF8 Flag setzen
		if ( $self->{utf8} and defined $lref ) {
			foreach my $p ( @{$lref} ) {
				Encode::_utf8_on($p);
			}
		}
                $sth->finish or croak "$exc:get\t$DBI::errstr\n$sql";
                return defined $lref ? @{$lref} : undef;
        } else {
                my $href = $sth->fetchrow_hashref;
                $sth->finish or croak "$exc:get\t$DBI::errstr\n$sql";
		return if not keys %{$href};
		my %lc_hash;
		map { Encode::_utf8_on($href->{$_}) if $self->{utf8};
		      $lc_hash{lc($_)} = $href->{$_} } keys %{$href};
                return \%lc_hash;
        }
}

# LEFT_OUTER_JOIN ----------------------------------------------------

sub left_outer_join {
        my $self = shift;

	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} ) {
		foreach my $p ( @_ ) {
			utf8::upgrade($p);
		}
	}
	
	return $self->db_left_outer_join (\@_);
}

# CMPI ---------------------------------------------------------------

sub cmpi {
        my $self = shift;
	
	my %par = @_;

	# Parametercheck
	
	croak "$exc:cmpi\tmissing col" unless defined $par{col};
	croak "$exc:cmpi\tmissing val" unless defined $par{val};
	croak "$exc:cmpi\tmissing op"  unless defined $par{op};

	croak "$exc:cmpi\tunknown op '$par{op}'"
		unless defined $known_operators{$par{op}};

	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} ) {
		utf8::upgrade($par{col});
		utf8::upgrade($par{val});
	}
	
	return $self->db_cmpi (\%par);
}

# USE_DB -------------------------------------------------------------

sub use_db {
        my $self = shift;
	
	my %par = @_;

	# Parametercheck
	
	croak "$exc:cmpi\tmissing db" unless defined $par{db};
	
	return $self->db_use_db (\%par);
}

# DB_PREFIX ----------------------------------------------------------

sub db_prefix {
        my $self = shift;
	
	my %par = @_;

	# Parametercheck
	
	croak "$exc:cmpi\tmissing db" unless defined $par{db};
	
	return $self->db_db_prefix (\%par);
}

# INSTALL ------------------------------------------------------------

sub install {
	my $self = shift;
	
	my %par = @_;
	
	eval {
		$self->db_install (\%par);
	};
	croak "$exc:install\t$@" if $@;
	
	1;
}

# LEFT_OUTER_JOIN ----------------------------------------------------

sub contains {
        my $self = shift;
	
	my %par = @_;
	
	croak "$exc:contains\tmissing col"        unless defined $par{col};
	croak "$exc:contains\tmissing vals"       unless defined $par{vals};
	croak "$exc:contains\tmissing search_op"  unless defined $par{search_op};

	croak "$exc:contains\tunsupported search_op '$par{search_op}'"
		if $par{search_op} ne 'sub';

	croak "$exc:contains\tmissing logic_op (number of vals > 1)"
		if @{$par{vals}} > 1 and not defined $par{logic_op};

	croak "$exc:contains\tunknown logic_op ($par{logic_op})"
		if defined $par{logic_op} and $par{logic_op} !~ /^(and|or)$/;

	# ggf. UTF8 Konvertierung vornehmen
	if ( $self->{utf8} ) {
		foreach my $p ( @{$par{vals}} ) {
			utf8::upgrade($p);
		}
	}
	
	$self->db_contains (\%par);
}

# GET_FEATURES -------------------------------------------------------

sub get_features {
	my $self = shift;
	
	return $self->{db_features};
}

1;

__END__

=head1 NAME

Dimedis::Sql - SQL/DBI Interface f�r datenbankunabh�ngige Applikationen

=head1 SYNOPSIS

  use Dimedis::Sql;

  # Konstruktor und Initialisierung
  my $sqlh = new Dimedis::Sql ( ... );
  $sqlh->install ( ... );

  # Ausf�hrung elementarer Kommandos zur Datenein-/ausgabe
  my $seq_id = $sqlh->insert ( ... );
  my $modified = $sqlh->update ( ... );
  my $blob_sref = $sqlh->blob_read ( ... );

  # Handling mehrerer Datenbanken
  $sqlh->use_db ( ...)
  my $db_prefix = $sqlh->db_prefix ( ...)

  # direkte Ausf�hrung von SQL Statements
  my $modified = $sqlh->do ( ... );
  my $href = $sqlh->get ( ... );
  my @row  = $sqlh->get ( ... );

  # Generierung von datenbankspezifischem SQL Code
  my ($from, $where) = $sqlh->outer_join ( ... );
  my $cond = $sqlh->cmpi ( ... );
  my $where = $sqlh->contains ( ... );

  # Kompatibilit�tspr�fung
  my $feature_href = $sqlh->get_features;

=head1 DESCRIPTION

Dieses Modul erleichtert die Realisierung datenbankunabh�ngiger
Applikationen. Die Schnittstelle gliedert sich in drei Kategorien:

=over 4

=item B<Ausf�hrung elementarer Kommandos zur Datenein-/ausgabe>

Diese Methoden f�hren anhand vorgegebener Parameter intern generierte
SQL Statements direkt �ber das DBI Modul aus. Die Parameter sind dabei
so abstrakt gehalten, da� sie von jeglicher Datenbankarchitektur
unabh�ngig sind.

=for html
<P>

=item B<direkte Ausf�hrung von SQL Statements>

Die Methoden dieser Kategorie f�hren SQL Statements ohne weitere
Manipulation direkt �ber das DBI Modul aus. Diese Statements m�ssen
also von ihrer Art her bereits unabh�ngig von jeglicher verwendeten
Datenbankarchitektur sein.

=for html
<P>

=item B<Generierung von datenbankspezifischen SQL Code>

Diese Methoden f�hren keine Statements aus sondern generieren anhand
gegebener Parameter den SQL Code f�r eine bestimmte Datenbankplattform
und geben diesen zur�ck, so da� er mit den Methoden der ersten beiden
Kategorien weiterverarbeitet werden kann.

=back

=head1 VORAUSSETZUNGEN

Es gibt einige Voraussetzungen f�r erfolgreiche datenbankunabh�ngige
Programmierung mit diesem Modul.

=over 4

=item B<Verwendung datenbankspezifischer Datentypen>

Es d�rfen keine datenbankspezifischen Datentypen verwendet werden,
die nicht von diesem Modul erfa�t sind.

Besonderheiten der unterschiedlichen Datenbankplattformen und wie
Dimedis::Sql damit umgeht, k�nnen der Dokumentation des entsprechenden
Datenbanktreibers (Dimedis::SqlDriver::*) entnommen werden.

=for html
<P>

=item B<Konvention f�r das Datum Format>

Die von der Datenbank gegebenen Typen f�r die Speicherung von Zeit- und
Datum Werten d�rfen nicht verwendet werden. Stattdessen mu� ein
String von folgendem Format verwendet werden:

B<YYYYMMDDHH:MM:SS>

=for html
<P>

=item B<Grunds�tzliche Kenntnisse im Umgang mit DBI>

Dieses Modul bildet alle Operationen direkt auf die darunter liegende
DBI Schnittselle ab. Deshalb werden Grundkenntnisse der DBI Programmierung
vorausgesetzt, z.B. die Technik des Parameter Bindings. Bei Problemen
kann die manpage des DBI Moduls (perldoc DBI) u.U. weiterhelfen.

=back

=head1 VERWENDUNG VON FILEHANDLES UNTER WINDOWS

Bei der Verwendung des Moduls unter Windows ist folgendes grunds�tzlich
zu beachten: beim Umgang mit Bin�rdateien unter Windows ist es erforderlich,
da� s�mtlicher File I/O im 'binmode' durchf�hrt wird, d.h. die
f�r die entsprechenden Filehandles mu� die Perl Funktion binmode
aufgerufen werden.

Dimedis::Sql ruft grunds�tzlich f�r B<alle> Filehandles binmode auf,
auch wenn diese vom Benutzer �bergeben wurden. Dies stellt kein
Problem dar, wenn in vom Benutzer �bergebene Filehandles noch nichts
geschrieben bzw. gelesen wurde.

Wenn Filehandles �bergeben werden, die bereits f�r I/O verwendet wurden,
f�hrt dies zu undefinierten Zust�nden, wenn diese nicht bereits vorher
mit binmode behandelt wurden. Deshalb m�ssen Filehandles, die vor der
�bergabe an Dimedis::Sql bereits verwendet werden sollen,  B<unbedingt>
sofort nach dem �ffnen mit binmode in den Bin�rmodus versetzt werden.

=head1 FEHLERBEHANDLUNG

Alle Methoden erzeugen im Fehlerfall eine Exception mit der Perl B<croak>
Funktion. Die Fehlermeldung hat folgenden Aufbau:

  "$method\t$message"

Dabei enth�lt $method den vollst�ndigen Methodennamen und
$message eine detailliertere Fehlermeldung (z.B. $DBI::errstr, wenn
es sich um einen SQL Fehler handelt).

=head1 CACHING VON SQL BEFEHLEN

DBI bietet ein Feature an, mit dem einmal ausgef�hrte SQL Statements
intern gecached werden. Bei einem gecachten Statement entf�llt der
Aufwand f�r das 'prepare'. Dies kann (insbesondere im Kontext persistenter
Perl Umgebungen) erhebliche Performancevorteile bringen, allerdings
auf Kosten des Speicherverbrauchs.

Grunds�tzlich benutzt Dimedis::Sql wo m�glich dieses Caching Feature. Es
gibt aber Gr�nde, es nicht zu verwenden. Wenn es nicht m�glich ist, alle
Parameter eines Statements mit Parameter Binding zu �bergeben, sollte das
resultierende Statement B<nicht> gecached werden. Der eingebettete Parameter
w�rde mit gecached werden. Die Wahrscheinlichkeit aber, da� dieses Statement
genau B<so> noch einmal abgesetzt wird, ist extrem gering. Daf�r wird aber
viel Speicher verbraucht, weil das gecachte Statement bis zur Proze�beendung
im Speicher verbleibt. Zudem gibt es bei den verschiedenen Datenbanken
eine Begrenzung der gleichzeitig offenen Statement-Handles.

Bei einigen Methoden und beim Konstruktor gibt es deshalb einen B<cache>
Parameter, um die Verwendung des Caches zu steuern.

Der B<cache> Parameter gibt an, das DBI Statement Caching verwendet werden soll,
oder nicht. In der Regel erkennt Dimedis::Sql selbst�ndig, ob das
Statement cachebar ist oder nicht: wenn keine B<params> zwecks Parameter
Binding �bergeben wurden, so wird das Statement nicht gecached, weil davon
ausgegangen wird, da� entsprechende Parameter im SQL Befehlstext direkt
eingebettet sind, was ein Caching des SQL Befehls sinnlos macht. Andernfalls
cached Dimedis::Sql das Statement immer.

�ber den B<cache> Parameter kann der Anwender das Verhalten selbst steuern.
Falls cache => 0 beim Erzeugen der Dimedis::Sql Instanz angegeben wurde,
ist das Caching B<immer> abgeschaltet, unabh�ngig von den oben beschriebenen
Bedingungen. B<ACHTUNG>: derzeit unterst�tzen nicht alle Dimedis::SqlDriver
dieses Feature (sowohl das globale Abschalten des Caches, als
auch das Einstellen pro Methodenaufruf). $sqlh->get_features gibt hier�ber
Auskunft. Wenn der B<cache> Parameter nicht unterst�tzt wird, so ist nicht
definiert, ob mit oder ohne Cache gearbeitet wird.

=head1 UNICODE SUPPORT

Unter Perl 5.8.0 unterst�tzt Dimedis::Sql auch Unicode. Beim Konstruktor
mu� dazu das utf8 Attribut gesetzt werden. Dimedis::Sql konvertiert damit
alle Daten (au�er Blobs) ggf. in das UTF8 Format, wenn die Daten nicht bereits
in UTF8 vorliegen.

Alle gelesenen Daten erhalten das Perl eigene UTF8 Flag gesetzt, d.h. es
wird vorausgesetzt, da� alle in der Datenbank gespeicherten Daten auch im
UTF8 Format vorliegen. Solange Dimedis::Sql stets im UTF8 Modus betrieben,
ist das auch gew�hrleistet. Eine Mischung von UTF8- und nicht-UTF8-Daten
ist nicht m�glich und f�hrt zu fehlerhaft codierten Daten.

Der UTF8 Support ist datenbankabh�ngig (derzeit unterst�tzt von MySQL
und Oracle). Das B<get_features> Hash hat einen Eintrag B<utf8>, der
angibt, ob die Datenbank UTF8 unterst�tzt, oder nicht.

=head1 BEHANDLUNG VON LEER-STRINGS / NULL SPALTEN

Leer-Strings werden von den Datenbanksystemen unterschiedlich behandelt.
Einige konvertieren sie stets zu NULL Spalten, andere k�nnen zwischen
NULL und Leer-String korrekt unterscheiden.

Zur Erf�llung eines minimalen Konsens werden alle Leerstrings von den
Dimedis::Sql Methoden zu undef bzw. NULL konvertiert, so da� es
grunds�tzlich keine Leerstrings gibt, sondern nur NULL Spalten bzw.
undef Werte (NULL wird in DBI durch undef repr�sentiert).

=head1 METHODEN

=head2 KONSTRUKTOR

  my $sqlh = new Dimedis::Sql (
  	dbh          => $dbh
     [, debug        => {0 | 1} ]
     [, cache        => {0 | 1} ]
     [, serial_write => {0 | 1} ]
     [, utf8         => {0 | 1] ]
  );

Der Konstruktor erkennt anhand des �bergebenen DBI Handles die
Datenbankarchitektur und l�dt das entsprechende Dimedis::SqlDriver
Modul f�r diese Datenbank, welches die �brigen Methoden implementiert.

Wenn der B<debug> Parameter gesetzt ist, werden Debugging Informationen
auf STDERR geschrieben. Es gibt keine Unterscheidung in unterschiedliche
Debugging Levels. Generell werden alle ausgef�hrten SQL Statements
ausgegeben sowie zus�tzliche spezifische Debugging Informationen, je
nach verwendeter Funktion.

�ber den B<cache> Parameter kann das DBI Caching von prepared Statements
gesteuert werden. Wenn hier 0 �bergeben wird, werden Statements grunds�tzlich
nie gecached (auch wenn bei einigen Statements lokal explizit cache => 1
gesetzt wurde. So kann das Caching bei Problemen sehr leich an zentraler
Stelle abgeschaltet werden. Default ist eingeschaltetes Caching.

Der B<serial_write> Parameter gibt an, ob explizite Werte f�r serial
Spalten angegeben werden d�rfen. Per Default ist dies verboten.

Der B<utf8> Parameter schaltet das Dimedis::Sql Handle in den UTF8 Modus.
Siehe das Kapitel UNICODE SUPPORT.

=head2 EINSCHR�NKUNGEN

Parameter f�r eine like Suche k�nnen nicht via Parameter Binding
�bergeben werden (zumindest Sybase unterst�tzt dies nicht).

=head2 �FFENTLICHE ATTRIBUTE

Es gibt einige Attribute des $sqlh Handles, die direkt verwendet
werden k�nnen:

=over 4

=item $sqlh->{dbh}

Dies ist das DBI database handle, das dem Konstruktor �bergeben
wurde. Es darf read only verwendet werden.

=for html
<P>

=item $sqlh->{debug}

Das Debugging-Verhalten kann jederzeit durch direktes Setzen
auf true oder false kontrolliert werden.

=for html
<P>

=item $sqlh->{db_type}

Dieses Read-Only Attribut enth�lt den verwendeten Datenbanktreiber.
Hier sind derzeit folgende Werte m�glich:

  Oracle
  Informix
  Sybase

=item $sqlh->{serial_write}

Der B<serial_write> Parameter gibt an, ob explizite Werte f�r serial
Spalten angegeben werden d�rfen. Per Default ist dies verboten.

=item $sqlh->{utf8}

Das B<utf8> Attribut gibt an, ob das Dimedis::Sql Handle im UTF8
Modus ist, oder nicht. Das Attribut ist read-only. Eine Datenbank
kann nur als ganzes in UTF8 betrieben werden, oder gar nicht. Ein
Mischbetrieb mit anderen Zeichens�tzen ist nicht m�glich.

=back 4

=head2 INITIALISIERUNG

  $sqlh->install

Diese Methode mu� nur einmal bei der Installation der Applikation
aufgerufen werden. Sie erstellt in der Datenbank Objekte, die von dem
entsprechenden datenbankabh�ngigen SqlDriver ben�tigt werden.

Es ist m�glich, da� ein SqlDriver keine Objekte in der Datenbank
ben�tigt, dann ist seine install Methode leer. Trotzdem mu� diese
Methode B<immer> bei der Installation der Applikation einmal
aufgerufen werden.

=head2 DATEN EINF�GEN

  my $seq_id = $sqlh->insert (
  	table	=> "table_name",
	data	=> {
		col_j => $val_j,
		...
	},
	type	=> {
		col_i => 'serial',
		col_j => 'date',
		col_k => 'clob',
		col_l => 'blob',
		...
	}
  );

Die insert Methode f�gt einen Datensatz in die angegebene Tabelle
ein. Der R�ckgabewert ist dabei eine evtl. beim Insert generierte
Primary Key ID.

Die einzelnen Werte der Spalten werden in dem B<data> Hash �bergeben.
Dabei entsprechen die Schl�ssel des Hashs den Spaltennamen der
Tabelle, deren Namen mit dem B<type> Parameter �bergeben wird. SQL
B<NULL> Werte werden mit dem Perl Wert B<undef> abgebildet.

Das B<type> Hash typisiert alle Spalten, die keine String oder Number
Spalten sind. Hier sind folgende Werte erlaubt:

=over 4

=item serial

Diese Spalte ist ein numerischer Primary Key der Tabelle, deren
Wert bei Bedarf automatisch vergeben word.

Der serial Datentyp darf nur einmal pro Insert vorkommen.

Um eine serial Spalte mit den automatisch generierten Wert zu setzen,
mu� im data Hash hierf�r undef �bergeben werden. Wenn eine serial
Spalte auf einen fixen Wert gesetzt werden soll, so mu� im data
Hash der entsprechende Wert �bergeben werden.

B<Beispiel:>

  my $id = $sqlh->insert (
  	table => 'users',
	data => {
		id => undef,
		nickname => 'foo'
	},
	type => {
		id => 'serial'
	}
  );

In diesem Beispiel wird ein Datensatz in die Tabelle 'users' eingef�gt,
die eine serial Spalte enth�lt. Die Spalte 'nickname' wird im B<type>
Hash nicht erw�hnt, da es sich hierbei um eine CHAR Spalte handelt.

=for html
<P>

=item date

Diese Spalte ist vom Typ Datum. Dimedis::Sql nimmt bei Werten dieses
Typs eine Pr�fung auf syntaktische Korrektheit vor. Es wird B<nicht>
gepr�ft, ob es sich dabei um ein B<g�ltiges> Datum handelt, sondern
lediglich, ob das Zahlenformat eingehalten wurde.

=for html
<P>

=item clob blob

Es gibt zwei M�glichkeiten einen BLOB oder CLOB einzuf�gen. Wenn das Objekt
im Speicher vorliegt, wird eine Scalar-Referenz im data Hash erwartet. Wenn
ein Skalar �bergeben wird, wird dieses als vollst�ndiger
Dateiname interpretiert und die entsprechende Datei in die Datenbank
eingef�gt. Die Datei wird dabei nicht gel�scht, sondern bleibt erhalten.

B<Zus�tzlich gilt folgende Einschr�nkung f�r BLOBs:>

  - die Verarbeitung von BLOBS ist nur m�glich, wenn
    eine serial Spalte mit angegeben ist

B<Beispiel:>

Hier wird ein Blob aus einer Datei heraus eingef�gt:

  my $id = $sqlh->insert (
  	table => 'users',
	data => {
		id => undef,
		nickname => 'foo',
		photo => '/tmp/uploadfile'
	},
	type => {
		id => 'serial',
		photo => 'blob'
	}
  );

Hier wird dieselbe Datei eingef�gt, nur diesmal wird sie
vorher in den Speicher eingelesen, und dann aus dem Speicher
heraus in die Datenbank eingef�gt (�bergabe als Skalarreferenz):

  open (FILE, '/tmp/uploadfile')
    or die "can't open /tmp/uploadfile';
  binmode FILE;
  my $image = join ('', <FILE>);
  close FILE;

  my $id = $sqlh->insert (
  	table => 'users',
	data => {
		id => undef,
		nickname => 'foo',
		photo => \$image
	},
	type => {
		id => 'serial',
		photo => 'blob'
	}
  );

=back 4

=head2 DATEN UPDATEN

  my $modified = $sqlh->update (
  	table	=> "table_name",
	data	=> {
		col_j => $val_j,
		...
	},
	type	=> {
		col_j => 'date',
		col_k => 'clob',
		col_l => 'blob',
		...
	},
	where	=> "where clause"
     [, params  => [ $where_par_n, ... ] ]
     [, cache   => 1|0 ]
  );

Die update Methode f�hrt ein Update auf der angegebenen Tabelle durch.
Dabei werden Tabellenname, Daten und Typinformationen wie bei der
insert Methode �bergeben. Zus�tzlich wird mit dem B<where> Parameter
die WHERE Klausel f�r das Update angegeben, wobei optional mit
dem params Parameter Platzhalter Variablen f�r die where Klausel
�bergeben werden k�nnen. Das Wort 'where' darf in dem B<where> Parameter
nicht enthalten sein.

Der R�ckgabewert ist die Anzahl der von dem UPDATE ver�nderten Datens�tze.
Wenn B<nur> BLOB Spalten upgedated werden, ist der R�ckgabewert nicht
spezifiziert und kann je nach verwendeter Datenbankarchitektur variieren.

Der B<cache> Parameter wird im Kapitel B<CACHING VON SQL BEFEHLEN>
beschrieben.

Zus�tzlich zu den Einschr�nkungen der insert Methode mu� noch
folgendes beachtet werden:

=over 4

=item Serial Spalte

Serial Spalten k�nnen B<nicht> ver�ndert werden und d�rfen demzufolge
nicht an einem Update beteiligt sein.

=for html
<P>

=item BLOB Update

Zum Updaten eines BLOBs bedarf es demzufolge der serial Spalte nicht.
Daf�r B<mu�> die B<where> Bedingung aber eindeutig sein, d.h. sie
darf nur einen Datensatz liefern. Ein Update mehrerer Blobs mu� also
durch mehrere Aufrufe der update Methode gel�st werden.

Diese Einschr�nkung wird u.U. in Zukunft aufgehoben.

=back 4

B<Beispiel:>

In diesem Beispiel wird eine Blob Spalte upgedated, aus einer Datei
heraus. Der B<where> Parameter selektiert genau eine Zeile �ber
die B<id> Spalte der Tabelle. Der Wert der Spalte wird �ber Parameter
Binding �bergeben.

  $sqlh->update (
  	table => 'users',
	data => {
		photo => '/tmp/uploadfile'
	},
	type => {
		photo => 'blob'
	},
	where => 'id = ?',
	params => [ $id ]
  );

=head2 BLOBS LESEN

  my $blob_sref = $sqlh->blob_read (
  	table	 => "table_name",
	col	 => "blob_column_name",
	where	 => "where clause"
     [, params   => [ $par_j, ... ]          ]
     [, filename => "absolute_path"          ]
     [, filehandle => "filehandle reference" ]
  );

Mit der B<blob_read> Methode wird ein einzelner Blob (oder Clob) gelesen
und als Skalarreferenz zur�ckgegeben. Dabei werden Tabellennamen, Spaltenname
sowie die WHERE Klausel zum Selektieren der richtigen Zeile als Parameter
�bergeben.

Wenn der optionale Parameter filename gegeben ist, wird der Blob
nicht als Skalarreferenz zur�ckgegeben, sondern stattdessen in die
entsprechende Datei geschrieben und undef zur�ckgegeben.

Wenn filehandle angegeben ist, wird das Blob in diese Filehandle Referenz
geschrieben und undef zur�ckgegeben. Die mit dem Filehandle verbundene
Datei wird B<nicht> geschlossen.

filehandle und filename d�rfen nicht gleichzeitig angegeben werden.

B<Beispiel:>

In diesem Beispiel wird ein Blob in eine Variable eingelesen:

  my $blob_sref = $sqlh->blob_read (
  	table	 => "users",
	col	 => "photo",
	where	 => "id=?",
        params   => [$id],
  );

Dasselbe Blob wird nun auf STDOUT ausgegeben, beispielsweise um
ein GIF Bild an einen Browser auszuliefern (binmode f�r die Win32
Kompatibilit�t nicht vergessen!):

  binmode STDOUT;
  print "Content-type: image/gif\n\n";
  
  $sqlh->blob_read (
  	table	 => "users",
	col	 => "photo",
	where	 => "id=?",
        params   => [$id],
	filehandle => \*STDOUT
  );

=head2 SQL BEFEHLE ABSETZEN

  my $modified = $sqlh->do (
  	sql	=> "SQL Statement",
     [, params	=> [ $par_j, ... ] ]
     [, cache   => 0|1 ]
  );

Mit der do Methode wird ein vollst�ndiges SQL Statement ausgef�hrt, d.h.
ohne weitere Bearbeitung an DBI durchgereicht. Optionale Platzhalter
Parameter des SQL Statements werden dabei mit dem B<params> Parameter �bergeben.

Der B<cache> Parameter wird im Kapitel B<CACHING VON SQL BEFEHLEN>
beschrieben.

Der R�ckgabewert ist die Anzahl der von dem UPDATE ver�nderten Datens�tze.

=head2 DATEN LESEN

  my $href =
  my @row  = $sqlh->get (
  	sql	=> "SQL Statement",
     [, params	=> [ $par_j, ... ] ]
     [, cache   => 0|1 ]
  );

Die get Methode erm�glicht das einfache Auslesen einer Datenbankzeile
mittels eines vollst�ndigen SELECT Statements, d.h. das SQL Statement wird
ohne weitere Bearbeitung an DBI durchgereicht. Optionale Platzhalter
Parameter werden dabei mit dem params Parameter �bergeben.

Im Scalar-Kontext aufgerufen, wird eine Hashreferenz mit Spalte => Wert
zur�ckgegeben. Im Listen-Kontext wird die Zeile als Liste zur�ckgegeben.

Wenn das SELECT Statement mehr als eine Zeile liefert, wird nur die erste
Zeile zur�ckgeliefert und die restlichen verworfen. Eine Verarbeitung
von Ergebnismengen kann also mit der get Methode nicht durchgef�hrt werden.

Der B<cache> Parameter wird im Kapitel B<CACHING VON SQL BEFEHLEN>
beschrieben.

=head2 LEFT OUTER JOIN

  my ($from, $where) = $sqlh->left_outer_join (
	komplexe, teilweise verschachtelte Liste,
	Beschreibung siehe unten
  );

Diese Methode liefert g�ltige Inhalte von FROM und WHERE Klauseln zur�ck
(ohne die Schl�sselw�rte 'FROM' und 'WHERE'), die f�r die jeweilige
Datenbankplattform einen Left Outer Join realisieren. F�r die WHERE
Klausel wird B<immer> eine g�ltige Bedingung zur�ckgeliefert, sie kann
also gefahrlos mit "... AND $where" in ein SELECT Statement eingebunden
werden, ohne abzufragen, ob sich der Outer Join �berhaupt in der WHERE
Condition auswirkt.

Es wird eine Liste von Parametern erwartet, die einem der folgenden Schemata
gen�gen mu� (es werden zwei F�lle von Joins unterschieden). Unter
der Parameterzeile ist zum besseren Verst�ndnis jeweils die Umsetzung
f�r Informix und Oracle angedeutet.

(Es gibt noch einen weiteren Outer Join Fall, der von Dimedis::Sql aber
nicht unterst�tzt wird, da nicht alle Datenbankplattformen diesen
umsetzen k�nnen. Dabei handelt es sich um einen Simple Join, der als
gesamtes gegen die linke Tabelle left outer gejoined werden soll.)

=over 4

=item Fall I: eine odere mehrere Tab. gegen dieselbe linke Tab. joinen

Dieser Fall wird auch 'simple outer join' genannt.

  ("tableA A", ["tableB B"], "A.x = B.x" )
  
  Ifx:      A, outer B
  Ora:      A.x = B.x (+)

  Dies war ein Spezialfall des folgenden, es k�nnen also
  beliebig viele Tabellen jeweils mit A outer gejoined
  werden:

  ("tableA A", ["tableB B"], "A.x = B.x",
               ["tableC C"], "A.y = C.y",
               ["tableD D"], "A.z = D.z", ... )

  Ifx:      A, outer B, outer C
  Ora:      A.x = B.x (+) and A.y = C.y (+) and A.z = D.z (+) ...

=item Fall II: verschachtelter outer join

Dieser Fall wird auch 'nested outer join' genannt.

  ("tableA A",
   [ "tableB B", [ "tableC C" ], "B.y = C.y AND expr(c)" ],
   "A.x = B.x")

  Ifx:      A, outer (B, outer C)
  Ora:      A.x = B.x (+) and B.y = C.y (+)
            and expr(c (+) )

=item Beschreibung der Parameter�bergabe

Generell mu� die �bergebene Parameterliste den folgenden Regeln
gen�gen:

  - die Angabe einer Tabelle erfolgt nach dem Schema

    "Tabelle[ Alias]"

    Alle Spaltenbezeichner in den Bedinungen m�ssen den Alias
    verwenden (bzw. den Tabellennamen, wenn der Alias
    weggelassen wurde).

  - zu einem Left Outer Join geh�ren immer drei Bestandteile:

    1. linke Tabelle (deren Inhalt vollst�ndig bleibt)
    2. rechte Tabelle (in der fehlende Eintrage mit NULL
       gef�llt werden)
    3. Join Bedingung

    Die Parameterliste nimmt sie in genau dieser Reihenfolge
    auf, wobei die jeweils rechte Tabelle eines Outer Joins
    in eckigen Klammern steht:
    
    LeftTable, [ OuterRightTable ], Condition

    Dabei k�nnen im Fall I OuterRightTable und Condition
    beliebig oft auftreten, um die outer Joins dieser Tabellen
    gegen die LeftTable zu formulieren.
    
    Im Fall II erfolgt die Verschachtelung nach demselben
    Schema. Die OuterRightTable wird in diesem Fall zur
    LeftTable f�r den inneren Outer Join.

  - wenn zus�tzliche Spaltenbedingungen f�r eine rechte
    Tabelle gelten sollen, so m�ssen diese an die Outer
    Join Bedingung angeh�ngt werden, in der die Tabelle
    auch tats�chlich die rechte Tabelle darstellt.
    
    Im Fall II z.B. k�nnten sie theoretisch auch bei der
    Bedingung eines inneren Joins angegeben werden, das
    darf aber nicht geschehen, da die Tabelle im inneren
    Join als LeftTable fungiert. Dies f�hrt dann je nach
    Datenbankplattform nicht zu dem gew�nschten Resultat.
    
    Falsch:
    "A", ["B", ["C"], "B.y = C.y and B.foo=42"], "A.x = B.x"
    
    Richtig:
    "A", ["B", ["C"], "B.y = C.y"], "A.x = B.x and B.foo=42"

=back 4

=head2 CASE INSENSITIVE VERGLEICHE

  my $cond = $sqlh->cmpi (
  	col	=> "column_name",
	val	=> "column value (with wildcards)",
	op	=> 'like' | '=' | '!='
  );

Die cmpi Methode gibt eine SQL Bedingung zur�ck, die case insensitive ist.
Dabei gibt col den Namen der Spalte an und val den Wert der Spalte (evtl.
mit den SQL Wildcards % und ?, wenn der Operator like verwendet wird).
Der Wert mu� ein einfaches B<String Literal> sein, ohne umschlie�ende
Anf�hrungszeichen. Andere Ausdr�cke sind nicht erlaubt.

Der op Parameter gibt den Vergleichsoperator an, der verwendet werden soll.

Die cmpi Methode ber�cksichtigt eine mit setlocale() eingestellte Locale.

=head2 VOLLTEXTSUCHE

  my $cond = $sqlh->contains (
  	col	  => "column name",
	vals	  => [ "val1", ..., "valN" ],
      [ logic_op  => 'and' | 'or', ]
	search_op => 'sub'
  );

Die contains Methode generiert eine SQL Bedingung, die eine Volltextsuche
realisiert. Hierbei werden entsprechende datenbankspezifischen Erweiterungen
genutzt, die eine effeziente Volltextsuche erm�glichen (Oracle Context
Cartridge, Informix Excalibur Text Datablade).

col gibt die Spalte an, �ber die gesucht werden soll. vals zeigt auf die
Zeichenkette(n), nach der/denen gesucht werden soll (ohne Wildcards). Wenn
mit vals mehrere Werte �bergeben werden, mu� auch logic_op gesetzt sein,
welches bestimmt, ob die Suche mit 'and' oder 'or' verkn�pft werden soll.

Mit search_op k�nnen unterschiedliche Varianten der Volltextsuche spezifiert
werden. Z.Zt. kann hier nur 'sub' angegeben werden, um anzuzeigen, da� eine
Teilwortsuche durchgef�hrt werden soll.

Wenn eine Datenbank keine Volltextsuche umsetzen kann, wird undef
zur�ckgegeben.

=head2 DATENBANK WECHSELN

 $sqlh->use_db (
  	db	=> "database_name"
  );

Diese Methode wechselt auf der aktuellen Datenbankconnection zu
einer anderen Datenbank. Der Name der Datenbank wird mit dem
B<db> Parameter �bergeben.

=head2 DATENBANK TABELLEN PREFIX ERMITTELN

 $sqlh->db_prefix (
  	db	=> "database_name"
  );

Diese Methode liefert den Datenbanknamen zusammen mit dem
datenbankspezifischen Tabellen-Delimiter zur�ck. Der zur�ckgegebene
Wert kann direkt in einem SQL Statement zur vollst�ndigen
Qualifikation einer Tabelle verwendet werden, die in einer anderen
Datenbank liegt.

Beispiel:

  my $db_prefix = $sqlh->db_prefix ( db => 'test' );
  $sqlh->do (
    sqlh => 'update ${db_prefix}foo set bla=42'
  );

  Hier wird die Tabelle 'foo' in der Datenbank 'test'
  upgedated.


=head2 UNTERST�TZTE FEATURES

  my $feature_href = $sqlh->get_features;

Diese Methode gibt eine Hashreferenz zur�ck, die folgenden Aufbau
hat und beschreibt, welche Dimedis::Sql Features von der aktuell
verwendeten Datenbankarchitektur unterst�tzt werden:

  $feature_href = {
	serial 		=> 1|0,
	blob_read	=> 1|0,
	blob_write 	=> 1|0,
	left_outer_join => {
	    simple 	=> 1|0,
	    nested 	=> 1|0
	},
  	cmpi 		=> 1|0,
	contains 	=> 1|0,
	use_db 		=> 1|0,
	cache_control 	=> 1|0,
	utf8		=> 1|0,
  };

Sollten dem $feature_href Schl�ssel fehlen, so ist das
gleichbedeutend mit einem Setzen auf 0, d.h. das entsprechende
Feature wird nicht unterst�tzt.

'cache_control' meint die M�glichkeit, bei $sqlh->insert und
$sqlg->update mit dem Parameter 'cache' zu steuern, ob intern
mit Statement Caching gearbeitet werden soll, oder nicht.

=head1 AUTOR

Joern Reder, joern@dimedis.de

=head1 COPYRIGHT

Copyright (c) 1999 dimedis GmbH, All Rights Reserved

=head1 SEE ALSO

perl(1), Dimedis::SqlDriver::Oracle(3pm), Dimedis::SqlDriver::Informix(3pm)

=cut

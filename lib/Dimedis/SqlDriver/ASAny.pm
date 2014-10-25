package Dimedis::SqlDriver::ASAny;

use strict;
use vars qw($VERSION @ISA);

$VERSION = '0.03';
@ISA = qw(Dimedis::Sql);	# Vererbung von Dimedis::Sql

use Carp;
use File::Copy;
use FileHandle;

my $exc = "Dimedis::SqlDriver::ASAny:";	# Exception Prefix

my $BLOB_CHUNK_SIZE = 16382;

# offizielles Dimedis::SqlDriver Interface ===========================

# install ------------------------------------------------------------

sub db_install {
	my $self = shift;

	eval {
		# wir brauchen eine Tabelle f�r serials
		$self->do (
			sql => "create table dim_serial (
				  name	varchar(32) not null,
				  id 	integer default 0,
				  primary key(name)
				)"
		);
	};

	return 1;
}

# insert -------------------------------------------------------------

sub db_insert {
	my $self = shift;

	my ($par)= @_;
	$par->{db_action} = "insert";
	
	$self->db_insert_or_update ($par);
}

# update -------------------------------------------------------------

sub db_update {
	my $self = shift;

	my ($par)= @_;
	$par->{db_action} = "update";
	
	$self->db_insert_or_update ($par);
}

# blob_read ----------------------------------------------------------

sub db_blob_read {
        my $self = shift;

        my ($par) = @_;

        my $filename = $par->{filename};
        my $filehandle = $par->{filehandle};

        my $dbh = $self->{dbh};

	$dbh->{LongReadLen} = 4000000;

        # das ist einfach! rausSELECTen halt...

        my $sth = $dbh->prepare (
                "select $par->{col}
                 from   $par->{table}
                 where  $par->{where}"
        ) or croak "$DBI::errstr";

        $sth->execute(@{$par->{params}}) or croak $DBI::errstr;

        # Blob lesen

        my $ar = $sth->fetchrow_arrayref or croak $DBI::errstr;
        my $blob = $ar->[0];

        $sth->finish or croak $DBI::errstr;

        # und nun ggf. irgendwo hinschreiben...

        if ( $filename ) {
                open (BLOB, "> $filename") or croak "can't write $filename";
                binmode BLOB;
                print BLOB $blob;
                close BLOB;
                $blob = "";     # Speicher wieder freigeben
        } elsif ( $filehandle ) {
                binmode $filehandle;
                print $filehandle $blob;
                $blob = "";     # Speicher wieder freigeben
        }

        return \$blob;
}

# left_outer_join ----------------------------------------------------
{
	my $from;
	my $where;
	my %from_tables;

	sub db_left_outer_join {
		my $self = shift;
	
		# static Variablen initialisieren
		
		$from = "";
		$where = "";
		%from_tables = ();

		# Rekursionsmethode anwerfen

		$self->db_left_outer_join_rec ( @_ );

		# Dreck bereinigen
		
		$from =~ s/,$//;
		$where =~ s/ AND $//;

		return ($from, $where);
	}

	sub db_left_outer_join_rec {
		my $self = shift;

		my ($lref) = @_;
		
		# linke Tabelle in die FROM Zeile

		my $left = $lref->[0];
		$from .= $left."," unless $from_tables{$left};
		$from_tables{$left} = 1;
	
		if ( ref $lref->[1] ) {
			# aha, Outer Join
			if ( @{$lref->[1]} > 1 ) {
				# kein einfacher Outer Join
				# (verschachtelt oder outer join gegen
				#  simple join, Fall II/III)

				$where .= $self->db_join_cond (
					$left,
					$lref->[2],
					$lref->[1]->[0],
					'outer'
				)." AND ";
				$self->db_left_outer_join_rec ($lref->[1]);
			} else {
				# Fall I, outer join einer linken Tabelle
				# gegen eine oder mehrere rechte Tabellen
				my $i = 1;
				while ($i < @{$lref}) {
					$from  .= $lref->[$i]->[0].","
						unless $from_tables{$lref->[$i]->[0]};
					$from_tables{$lref->[$i]->[0]} = 1;
					$where .= $self->db_join_cond (
						$left,
						$lref->[$i+1],
						$lref->[$i]->[0],
						'outer'
					)." AND ";
					$i += 2;
				}
			}
		} else {
			# noe, kein Outer join
			die "$exc:db_left_outer_join\tcase III does not exist anymore";
			$from .= $lref->[1];
			$where .= $lref->[2]." AND ";
		}
	}
}

# cmpi ---------------------------------------------------------------

sub db_cmpi {
	my $self = shift;
	my ($par)= @_;

	use locale;

	return "lower($par->{col}) $par->{op} ".
	       $self->{dbh}->quote (lc($par->{val}));
}

# use_db -------------------------------------------------------------

sub db_use_db {
	my $self = shift;
	
	my ($par)= @_;

	$self->do (
		sql => "use $par->{db}",
		cache => 1
	);

	1;
}

# db_prefix ----------------------------------------------------------

sub db_db_prefix {
	my $self = shift;
	
	my ($par)= @_;

	return $par->{db}.'..';

	1;
}

# contains -----------------------------------------------------------

sub db_contains {
	my $self = shift;
	
	my ($par) = @_;
	my $cond;

	# bei ASAny z.Zt. nicht unterst�zt, deshalb undef returnen

	return $cond;
}

# get_features -------------------------------------------------------

sub db_get_features {
	my $self = shift;
	
	return {
		serial => 1,
		blob_read => 1,
		blob_write => 1,
		left_outer_join => {
			simple => 1,
			nested => 0
		},
	  	cmpi => 1,
		contains => 0,
		use_db => 1,
		cache_control => 1
	};
}

# Driverspezifische Hilfsmethoden ====================================

# join Bedingung bauen -----------------------------------------------

sub db_join_cond {
	my $self = shift;
	
	my ($left, $cond, $right, $join) = @_;
	
	# beim outer join m�ssen * Zeichen bei = Ausdr�cken der
	# linken Tabelle angeh�ngt werden
	
	my ($table, $alias) = split (/\s/, $left);
	$alias ||= $table;
	
	$cond =~ s/($alias\.[^\s]+)\s*=/$1 *=/g;
	
	return $cond;
}

# Serial ermitteln ---------------------------------------------------

sub db_get_serial {
	my $self = shift;
	
	my ($table) = @_;
	
	# Serials erzeugen wir selber, da der identity Mechanismus
	# von Sybase nicht im Zusammenhang mit Platzhaltern zu
	# gebrauchen ist (der zuletzt vergebene Wert kann nicht
	# ermittelt werden).
	
	# erstmal die Spalte (bzw. wohl leider die ganze
	# Tabelle) sperren, mit einem Pseudo Update.
	# (sonst k�nnten serials doppelt vergeben werden, da wir
	#  ja erst lesen und dann updaten m�ssen. Deshalb mu� dieser
	#  Vorgang in jedem Fall atomar ablaufen.)
		
	my $modified = $self->do (
		sql => "update dim_serial
		          set id=id
			where name=?",
		params => [ $table ]
	);
	
	# Hier kommt unsere serial rein.
	my $id;
	
	if ( $modified != 1 ) {
		# oha, die Zeile f�r unsere Tabelle gibt's noch
		# gar nicht: also anlegen!
		#
		# Wenn das gelingt, setzen wir $id auf 1,
		# wenn  nicht, war ein anderer Proze�
		# schneller und wir m�ssen uns den Wert
		# sp�ter noch rauslesen ($id bleibt erstmal
		# undef)
		
		eval {
			$self->do (
				sql => "insert into dim_serial
					(name, id)
					values
					(?, ?)",
				params => [ $table, 1 ]
			);
			$id = 1;
		};
	}
	
	# wenn $id noch undef, dann m�ssen wir uns den Wert
	# aus der Datenbank holen, eins hochz�hlen und
	# wieder wegschreiben
	
	if ( not $id ) {
		($id) = $self->get (
			sql => "select id
				from   dim_serial
				where  name=?",
			params => [ $table ]
		);
		++$id;
		$modified = $self->do (
			sql => "update dim_serial
				  set id=?
				where name=?",
			params => [ $id, $table ]
		);
		croak "Serial konnte nicht upgedated werden!"
			unless $modified == 1;
	}
	
	return $id;
}


# Insert bzw. Update durchf�hren -------------------------------------

sub db_insert_or_update {
	my $self = shift;

	$self->{debug} && print STDERR "$exc:db_insert_or_update entered\n";

	my ($par) = @_;
	my $type_href = $par->{type};
	my $par_cache = $par->{cache};

	my $serial;			# evtl. Serial Wert
	my (@columns, @values);		# Spaltennamen und -werte
	my $return_value;		# serial bei insert,
					# modified bei update
	
	# Parameter aufbereiten

	my ($col, $val);
	my @parameters;	# Parameter (Parameterbinding, falls moeglich)
	my %blobs;	# Hier werden BLOB Spalten abgelegt, die
			# nach dem INSERT eingef�gt werden
	my $blob_found;
	my $primary_key;	# Name der primary key Spalte
	my $qm;		# f�r ? Parameterbinding

	# Normalerweise werden Statements gecached,
	# es gibt aber auch Ausnahmen (z.B. bei globaler
	# Abschaltung oder gr��eren Texten, s.u.)
	my $cache = 1;

	if ( exists $par->{cache} ) {
		# oder der Benutzer will das Caching Verhalten
		# explizit selbst steuern
		# (wobei das sp�ter trotzdem noch abgeschaltet werden
		#  kann, z.B. bei gr��eren Texten, s.u.)
		$cache = $par_cache;
	}

	# wenn global abgeschaltet, dann bleibt's auch so
	$cache = 0 if not $self->{cache};

	while ( ($col,$val) = each %{$par->{data}} ) {
		my $type  = $type_href->{$col};
		$type     =~ s/\(([^\(]+)\)$//;
		my $scale = $1;
		
		$type     =~ s/\[.*//;

		if ( $type eq 'serial' ) {

			# serials generieren wir uns selber
			$return_value = $self->db_get_serial ($par->{table});
			push @columns,    $col;
			push @values,     $return_value;
			push @parameters, "?";
			$primary_key = $col;
			$qm .= '?,';

		} elsif ( $type eq 'blob' or $type eq 'clob' ) {

			# Blob mu� in jedem Fall im Speicher vorliegen

                        $val = $self->db_blob2memory($val);

                        # Blobs k�nnen inline geinsertet
                        # und updated werden
                        push @columns, $col;
                        push @values, $$val;
                        $qm .= "?,";

		} else {
			# alle �brigen Typen werden as is eingef�gt
			push @columns, $col;
			push @values,  $val;
			$qm .= "?,";
		}
	}
	$qm =~ s/,$//;  # letztes Komma b�geln
	
	# Insert oder Update durchf�hren
	
	if ( $par->{db_action} eq 'insert' ) {
		# insert ausf�hren

		$self->do (
			sql => "insert into $par->{table} (".
			       join (",",@columns).
			       ") values ($qm)",
			params => \@values,
			cache => $cache
		);
	} else {
		# Parameter der where Klausel in @value pushen
		push @values, @{$par->{params}};
		
		# update ausf�hren, wenn columns da sind
		# (bei einem reinen BLOB updated passiert es,
		#  da� keine 'normalen' Spalten upgedated werden)
		
                if ( @columns ) {
                        $return_value = $self->do (
                                sql => "update $par->{table} set ".
                                       join(",", map("$_=?", @columns)).
                                       " where $par->{where}",
                                params => \@values
                        );
                }
	}

	return $return_value;
}

# BLOB ins Memory holen, wenn nicht schon da -------------------------

sub db_blob2memory {
        my $self = shift;

        my ($val) = @_;

        my $blob;
        if ( ref $val and ref $val ne 'SCALAR' ) {
                # Referenz und zwar keine Scalarreferenz
                # => das ist ein Filehandle
                # => reinlesen den Kram
                binmode $val;
                $$blob = join ("", <$val>);
        } elsif ( not ref $val ) {
                # keine Referenz
                # => Dateiname
                # => reinlesen den Kram
                my $fh = new FileHandle;
                open ($fh, $val) or croak "can't open file '$val'";
                binmode $fh;
                $$blob = join ("", <$fh>);
                $self->{debug} && print STDERR "$exc:db_blob2memory: blob_size ($val): ", length($$blob), "\n";
                close $fh;
        } else {
                # andernfalls ist val eine Skalarreferenz mit dem Blob
                # => nix tun
                $blob = $val;
        }

        return $blob;
}

1;

__END__

=head1 NAME

Dimedis::SqlDriver::ASAny - SQL Anywhere Treiber f�r das Dimedis::Sql Modul

=head1 SYNOPSIS

siehe Dimedis::Sql

=head1 DESCRIPTION

siehe Dimedis::Sql

=head1 BESONDERHEITEN DER IMPLEMENTIERUNG

=head2 SERIAL BEHANDLUNG

Spalten, die mit dem 'serial' Datentyp angegeben sind, m�ssen in der
Datenbank als primary key integer Spalten deklariert sein, z.B.

        id integer primary key not null

Aufgrund von Restriktionen der Sybase Datenbank im Zusammenhang mit
Parameter Binding kann der Sybase eigene identity Mechanismus nicht
verwendet werden.

Stattdessen legt die $sqlh->install Methode eine eigene Tabelle f�r
die Verwaltung von serials an.

  create table dim_serial (
	  name	varchar(32),
	  id 	integer default 0,
	  primary key(name)
  )

Die Verwaltung dieser Tabelle erfolgt durch Dimedis::Sql vollkommen
transparent.

=head2 BLOB BEHANDLUNG

Blobs werden vollst�ndig �ber eine eigene Implementierung innerhalb
von Dimedis::Sql verwaltet. Hierzu legt die $sqlh->install Methode
eine Tabelle an, die Blobs in Form von einzelnen Datens�tzen mit
einer maximalen Gr��e von derzeit knapp 16KB aufnimmt.

  create table dim_blob (
	  id	integer not null, 
	  pos	integer not null,
	  chunk	image null,
	  primary key(id, pos)
  )

Eine Tabelle, die einen Blob aufnehmen soll, enth�lt nun nur eine
integer Referenz auf die ID des Blobs (dim_blob.id), da der Blob
selbst ja in der dim_blob Tabelle gespeichert wird. Der Zugriff
auf den Blob (lesend wie schreibend) wird durch Dimedis::Sql
vollkommen transparent durchgef�hrt.

Sybase Blobs machen keinen Unterschied zwischen textuellen (CLOB)
und bin�ren (BLOB) Daten.

Beispiel f�r eine Tabelle mit Blobs:

  create table test (
	id integer primary key not null,
	bild_blob integer null,
	text_blob integer null
  )

Damit Blobs, die auf diese Weise von einer Tabelle aus referenziert
werden, auch beim L�schen von Datens�tzen mit entfernt werden,
mu� ein Trigger angelegt werden, f�r jede Tabelle, die Blobs enth�lt:

  create trigger test_del
     on  test
     for delete as
	 delete from dim_blob
	 where  id in (select bild_blob from deleted) or
	        id in (select text_blob from deleted)

Die where Bedingung des 'delete' Statements mu� nat�rlich
entsprechend der Haupttabelle angepa�t werden. (Die 'deleted'
Tabelle in der where Bedingung ist eine Pseudo Tabelle, die
alle Datens�tze der Haupttabelle enth�lt, die im Begriff sind
durch das aktuell durchgef�hrte delete Statement entfernt zu
werden).

=head2 NULL VALUES

Dies ist keine Besonderheit von Dimedis::SqlDriver::Sybase,
sie betrifft Sybase Datenbanken grunds�tzlich. Per Default sind
Spalten in Sybase stets mit NOT NULL deklariert. Um eine Spalte
zu erzeugen, die NULL Werte aufnehmen darf, mu� explizit NULL
angegeben werden. (Siehe Beispieltabelle bei BLOB BEHANDLUNG).

=head2 LEFT OUTER JOIN METHODE

Sybase kennt keine nested outer Joins. Derzeit liefert Dimedis::Sql
zwar Werte f�r diesen Fall zur�ck, diese werden jedoch bei Ausf�hrung
von Sybase als fehlerhaft abgewiesen.

Nested outer Joins m�ssen demzufolge per Hand in der Applikation
realisiert werden. (Ob eine Datenbank nested outer Joins unterst�tzt,
kann mit der Methode $sqlh->get_features abgefragt werden).

=head2 CONTAINS METHODE

Diese Methode ist z.Zt. nicht implementiert, d.h. liefert B<immer> undef
zur�ck.

=head2 INSTALL METHODE

Die $sqlh->install Methode legt zwei Tabellen an, sofern sie
nicht schon existieren:

  dim_serial	verwaltet Serials (siehe SERIAL BEHANDLUNG)
  dim_blob	verwaltet Blobs (sie BLOB BEHANDLUNG)

=head1 AUTOR

J�rn Reder, joern@dimedis.de

=head1 COPYRIGHT

Copyright (c) 1999 dimedis GmbH, All Rights Reserved

=head1 SEE ALSO

perl(1).

=cut

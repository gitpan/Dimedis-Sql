# $Id: mysql.pm,v 1.9 2003/08/07 07:48:18 joern Exp $

package Dimedis::SqlDriver::mysql;

use strict;
use vars qw($VERSION @ISA);

$VERSION = '0.09';
@ISA = qw(Dimedis::Sql);	# Vererbung von Dimedis::Sql

use Carp;
use File::Copy;
use FileHandle;

my $exc = "Dimedis::SqlDriver::mysql:";	# Exception Prefix

# offizielles Dimedis::SqlDriver Interface ===========================

# install ------------------------------------------------------------

sub db_install {
	my $self = shift;
	
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
	
	# das ist einfach! rausSELECTen halt...

	my $sth = $dbh->prepare (
		"select $par->{col}
		 from   $par->{table}
		 where  $par->{where}"
	) or croak "$DBI::errstr";
		
	$sth->execute(@{$par->{params}}) or croak $DBI::errstr;

	# Blob lesen

	my $ar = $sth->fetchrow_arrayref;
	croak $DBI::errstr if $DBI::errstr;
	if ( not defined $ar ) {
		return \"";
	}

	my $blob = $ar->[0];

	$sth->finish or croak $DBI::errstr;
	
	# und nun ggf. irgendwo hinschreiben...	
	
	if ( $filename ) {
		open (BLOB, "> $filename") or croak "can't write $filename";
		binmode BLOB;
		print BLOB $blob;
		close BLOB;
		$blob = "";	# Speicher wieder freigeben
	} elsif ( $filehandle ) {
		binmode $filehandle;
		print $filehandle $blob;
		$blob = "";	# Speicher wieder freigeben
	}

	return if $par->{filehandle} or $par->{filename};
	return \$blob;
}

# left_outer_join ----------------------------------------------------
{
	my $from;
	my $where;

	sub db_left_outer_join {
		my $self = shift;
	
		# static Variablen initialisieren
		
		$from = "";
		$where = "";

		# Rekursionsmethode anwerfen

		$self->db_left_outer_join_rec ( @_ );
	
		# Dreck bereinigen

		$from =~ s/,$//;
		$from =~ s/,\)/)/g;
		$where =~ s/ AND $//;

		$where = '1=1' if $where eq '';

		return ($from, $where);
	}

	sub db_left_outer_join_rec {
		my $self = shift;

		my ($lref, $left_table_out) = @_;
		
		# linke Tabelle in die FROM Zeile

		$from .= " ".$lref->[0]
			if not $left_table_out;
		
		if ( ref $lref->[1] ) {
			# aha, Outer Join
			if ( @{$lref->[1]} > 1 ) {
				# kein einfacher Outer Join
				# (verschachtelt oder outer join gegen
				#  simple join, Fall II/III)

				$from .= " left outer join ".$lref->[1]->[0].
						 " on ".$lref->[2];

				$self->db_left_outer_join_rec ($lref->[1], 1);

			} else {
				# Fall I, outer join einer linken Tabelle
				# gegen eine oder mehrere rechte Tabellen
				my $i = 1;
				while ($i < @{$lref}) {
					$from .= " left outer join ".$lref->[$i]->[0].
						 " on ".$lref->[$i+1];
					$i += 2;
				}
			}
		} else {
			# noe, kein Outer join
			croak "$exc:db_left_outer_join\tcase III does not exist anymore";
			$from .= $lref->[1];
			$where .= $lref->[2]." AND ";
		}
	}
}

# cmpi ---------------------------------------------------------------

sub db_cmpi {
	my $self = shift;
	my ($par)= @_;

	my $not = $par->{op} eq '!=' ? 'not ' : '';

	my $quoted = $self->{dbh}->quote ($par->{val});

	# Bug in DBI->quote. utf8 flag ist weg :(
	# (wurde durch utf8::upgrade in ->cmpi gesetzt)
	Encode::_utf8_on($quoted) if $self->{utf8};

	return "$not$par->{col} like $quoted";
}

# use_db -------------------------------------------------------------

sub db_use_db {
	my $self = shift;
	
	my ($par)= @_;

	$self->do (
		sql => "use $par->{db}"
	);

	1;
}

# db_prefix ----------------------------------------------------------

sub db_db_prefix {
	my $self = shift;
	
	my ($par)= @_;

	return $par->{db}.'.';

	1;
}

# contains -----------------------------------------------------------

sub db_contains {
	my $self = shift;
	
	my ($par) = @_;
	my $cond;

	# bei Informix z.Zt. nicht unterstüzt, deshalb undef returnen

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
			nested => 1
		},
	  	cmpi => 1,
		contains => 0,
		use_db => 1,
		utf8 => 1,
	};
}

# Driverspezifische Hilfsmethoden ====================================

# Insert bzw. Update durchführen -------------------------------------

sub db_insert_or_update {
	my $self = shift;

	my ($par) = @_;
	my $type_href = $par->{type};

	my $serial;			# evtl. Serial Wert
	my (@columns, @values);		# Spaltennamen und -werte
	my $return_value;		# serial bei insert,
					# modified bei update
	
	# Parameter aufbereiten

	my ($col, $val);
	my $qm;		# Fragezeichen für Parameterbinding
	my %blobs;	# Hier werden BLOB Spalten abgelegt, die
			# nach dem INSERT eingefügt werden
	my $blob_found;
	
	while ( ($col,$val) = each %{$par->{data}} ) {
		my $type = $type_href->{$col};
		$type =~ s/\[.*//;

		if ( $type eq 'serial' and not defined $val ) {
			# serial Typ bearbeiten
			push @columns, $col;
			push @values, 0;
			$qm .= "?,";
			
		} elsif ( $type eq 'blob' or $type eq 'clob' ) {

			# Blob muß in jedem Fall im Speicher vorliegen
			$val = $self->db_blob2memory($val);

			# Ggf. UTF8 draus machen
			if ( $self->{utf8} and $type_href->{$col} eq 'clob' ) {
				utf8::upgrade($$val);
			}

			# Blobs können inline geinsertet 
			# und updated werden
			push @columns, $col;
			push @values, $$val;
			$qm .= "?,";

		} else {
			# Ggf. UTF8 draus machen
			utf8::upgrade($val) if $self->{utf8};

			# alle übrigen Typen werden as is eingefügt
			push @columns, $col;
			push @values,  $val;
			$qm .= "?,";
		}
	}
	$qm =~ s/,$//;	# letztes Komma bügeln
	
	# Insert oder Update durchführen
	
	if ( $par->{db_action} eq 'insert' ) {
		# insert ausführen

		$self->do (
			sql => "insert into $par->{table} (".
			       join (",",@columns).
			       ") values ($qm)",
			params  => \@values,
			no_utf8 => 1,	# Das haben wir schon gemacht,
					# außer bei Blobs. Die werden bei
					# MySQL as-is eingefügt, aber
					# dürfen natürlich *nicht* nach
					# UTF8 konvertiert werden
			
		);
		
		$return_value = $self->{dbh}->{'mysql_insertid'};
		
	} else {
		# Parameter der where Klausel in @value pushen
		push @values, @{$par->{params}};
		
		# update ausführen, wenn columns da sind
		# (bei einem reinen BLOB updated passiert es,
		#  daß keine 'normalen' Spalten upgedated werden)
		
		if ( @columns ) {
			$return_value = $self->do (
				sql => "update $par->{table} set ".
				       join(",", map("$_=?", @columns)).
				       " where $par->{where}",
				params => \@values,
				no_utf8 => 1,
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

Dimedis::SqlDriver::mysql - MySQL Treiber für das Dimedis::Sql Modul

=head1 SYNOPSIS

use Dimedis::SqlDriver;

=head1 DESCRIPTION

siehe Dimedis::Sql

=head1 BESONDERHEITEN DER IMPLEMENTIERUNG

=head2 SERIAL BEHANDLUNG

Spalten, die mit dem 'serial' Datentyp deklariert sind, müssen in der
Datenbank als primary key serial Spalten deklariert sein, z.B.

        id serial not null primary key

=head2 BLOB BEHANDLUNG

Es wird davon ausgegangen, daß Blob Spalten als 'mediumblob' deklariert
sind. Ansonsten gibt es keine besonderen Einschänkungen in der Blob
Behandlung.

=head2 INSTALL METHODE

Für Dimedis::SqlDriver::MySQL ist die install Methode leer,
d.h. es werden keine Objekte in der Datenbank vorausgesetzt.

=head2 CONTAINS METHODE

Diese Methode ist z.Zt. nicht implementiert, d.h. liefert B<immer> undef
zurück.

=head1 AUTOR

Jörn Reder, joern@dimedis.de

=head1 COPYRIGHT

Copyright (c) 2000 dimedis GmbH, All Rights Reserved

=head1 SEE ALSO

perl(1).

=cut

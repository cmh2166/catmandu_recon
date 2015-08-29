#!/usr/bin/env perl
#
# Match 100,110,700,710,650,655 against LCNAF, LCSH, AAT
#
# this is a watery mirror of the genius that is Patrick Hochstenbach <Patrick.Hochstenbach@UGent.be>
# his work (which sparked the below) on this here: https://gist.github.com/phochste/c87c81c79d8b8a6a2179
#
# the data creature that wrote the following: christina harlow, @cm_harlow on twitter
#
$|++;

use Catmandu::Sane;
use Catmandu;
use Catmandu::Fix::Inline::marc_map qw(:all);
use Catmandu::Exporter::MARC;
use RDF::LDF;
use Data::Dumper;
use Cache::LRU;
use Getopt::Long;

Catmandu->load(':up');

my $store     = undef;
my $type      = 'USMARC';
my $fix       = undef;

GetOptions("fix=s" => \$fix , "store=s" => \$store , "type=s" => \$type);
my $query     = shift;

unless ($query) {
    print STDERR <<EOF;
usage: $0 [--fix fix] [--type USMARC|JSON|XML|RAW|ALEPHSEQ] file
usage: $0 [--fix fix] [--store store] query
usage: $0 [--fix fix] [--store store] all
EOF
    exit(1);
}

$query = undef if $query eq 'all';

my $lcsh_endpoint = 'http://localhost:5000/lcsh';
my $lcnaf_endpoint = 'http://localhost:5000/lcnaf';
my $aat_endpoint = 'http://localhost:5000/aat';
my $lcsh_client    = RDF::LDF->new(url => $lcsh_endpoint);
my $lcnaf_client    = RDF::LDF->new(url => $lcnaf_endpoint);
my $aat_client    = RDF::LDF->new(url => $aat_endpoint);
my $cache     = Cache::LRU->new(size => 10000);

my $iterator;

if (-r $query) {
    $iterator  = Catmandu->importer('MARC',file => $query, type => $type);
}
else {
    $iterator  = Catmandu->store($store)->bag->searcher(query => $query);
}

my $exporter = Catmandu::Exporter::MARC->new(file => "marc.mrc", type => "USMARC" );

binmode(STDOUT,':encoding(UTF-8)');

&do_import($fix,$query);

sub do_import {
    my $fix   = shift;
    my $query = shift;
    my $fixer;

    $fixer = Catmandu->fixer($fix) if defined $fix;

    my $n = $iterator->each(sub {
        my $item      = shift;
        my $id        = $item->{_id};

        my $record    = $item->{record};

        for my $field (@$record) {
            my ($tag,$ind1,$ind2,@data) = @$field;
            if ($tag eq '100' || $tag eq '700') {
                my $doc = { record => [['100',$ind1,$ind2,@data]] };
                my @aut  = flat(marc_map($doc,'100adeqt0' , -split => 1 , -pluck => 1));

                if (@aut == 0) {
                    next;
                }

                my ($name,$date,$relator,$fullerName,$title,$valueID) = @aut;
                $name =~ s/,$//;

                if ((defined $date && $date =~/^(\d{4}-(\d{4})?)$/) || (defined $fullerName) && ! defined $title && ! defined $valueID) {
                    my @res = &get_personalName_id($name,$date,$fullerName);
                    if (@res == 1) {
                        my $uri = pop @res;
                        push @$field, "0";
                        push @$field, "$uri";
                    } else {
                        my $num = int(@res);
                        # more than one hit
                    }
                } else {
                    next;
                }

            } elsif ($tag eq '110' || $tag eq '710') {
                my $doc = { record => [['110',$ind1,'$ind2',@data]] };
                my @corp  = flat(marc_map($doc,'110at0' , -split => 1 , -pluck => 1));

                if (@corp == 0) {
                    next;
                }

                my ($name,$title,$valueID) = @corp;
                $name =~ s/,$//;

                if (! defined $title && ! defined $valueID) {
                    my @res = &get_corpName_id($name);
                    if (@res == 1) {
                        my $uri = pop @res;
                        push @$field, "0";
                        push @$field, "$uri";
                    } else {
                        my $num = int(@res);
                        # more than one hit
                    }
                } else {
                    next;
                }

            } elsif ($tag eq '650') {
                my $doc = { record => [['650',$ind1,'0',@data]] };
                my @subj  = flat(marc_map($doc,'650abcdevxyz0' , -split => 1 , -pluck => 1));
                my @subjID  = flat(marc_map($doc,'650a0' , -split => 1 , -pluck => 1));

                if (@subj == 0) {
                    next;
                }

                my ($test,$identifier) = @subjID;
                my ($term,$altTerm,$location,$dates,$relator,$form,$subdiv,$chron,$geo,$URI) = @subj;
                my $subjTerm = join('--',@subj);
                $subjTerm =~ s/.$//;

                if (not defined $URI) {
                    my @res = &get_subj_id($subjTerm);
                    if (@res == 1) {
                        my $uri = pop @res;
                        push @$field, "0";
                        push @$field, "$uri";
                    } else {
                        my $num = int(@res);
                        # more than one hit
                    }
                } else {
                    next;
                }
            } elsif ($tag eq '655') {
                my $doc = { record => [['655',$ind1,'0',@data]] };
                my @genre  = flat(marc_map($doc,'655a0' , -split => 1 , -pluck => 1));

                if (@genre == 0) {
                    next;
                }

                my ($genre,$identifier) = @genre;
                $genre =~ s/.$//;

                if (not defined $identifier) {
                    my @res = &get_aat_id($genre);
                    if (@res == 1) {
                        my $uri = pop @res;
                        push @$field, "0";
                        push @$field, "$uri";
                    } else {
                        my $num = int(@res);
                        # more than one hit
                    }
                } else {
                    next;
                }
            } else {
                next;
            }

        }
        $exporter->add($item);
        $exporter->commit;
    });

    print STDERR "Processed $n records\n";
}

sub marcseq {
    my (@data) = @_;

    my $str = "";
    for (my $i = 0 ; $i < @data ; $i += 2) {
        if ($data[$i] eq '_') {
            $str .= $data[$i+1];
        }
        else {
            $str .= $data[$i] . $data[$i+1];
        }
    }
    $str;
}

sub get_personalName_id {
    my ($name,$date,$fullerName) = @_;

    my $key='';
    if(defined $date && not defined $fullerName) {
        $key = "\"$name, $date\"\@en";
    }
    elsif (not defined $date && defined $fullerName) {
        $key = "\"$name, $fullerName\"\@en";
    } else {
        $key = "\"$name, $fullerName, $date\"\@en";
    }

    if (defined(my $value = $cache->get($key))) {
        return @$value;
    }
    else {
        my $value = &lcnaf_query($key);
        $cache->set($key => $value);
        return @$value;
    }
}

sub get_corpName_id {
    my ($name) = @_;

    my $key='';
    $key = "\"$name\"\@en";

    if (defined(my $value = $cache->get($key))) {
        return @$value;
    }
    else {
        my $value = &lcnaf_query($key);
        $cache->set($key => $value);
        return @$value;
    }
}

sub get_subj_id {
    my ($subj) = @_;

    my $key='';
    $key = "\"$subj\"\@en";

    if (defined(my $value = $cache->get($key))) {
        return @$value;
    }
    else {
        my $value = &lcsh_query($key);
        $cache->set($key => $value);
        return @$value;
    }
}

sub get_aat_id {
    my ($genre) = @_;

    my $key='';
    $key = "\"$genre\"\@en";

    if (defined(my $value = $cache->get($key))) {
        return @$value;
    }
    else {
        my $value = &aat_query($key);
        $cache->set($key => $value);
        return @$value;
    }
}

sub lcnaf_query {
    my $object = shift;
    my $it = $lcnaf_client->get_statements(undef, 'http://www.w3.org/2004/02/skos/core#prefLabel', $object);

    my @res = ();
    while (my $st = $it->()) {
        push @res, $st->subject->uri;
    }

    return \@res;
}

sub lcsh_query {
    my $object = shift;
    my $it = $lcsh_client->get_statements(undef, 'http://www.w3.org/2004/02/skos/core#prefLabel', $object);

    my @res = ();
    while (my $st = $it->()) {
        push @res, $st->subject->uri;
    }

    return \@res;
}

sub aat_query {
    my $object = shift;
    my $it = $aat_client->get_statements(undef, 'http://www.w3.org/2004/02/skos/core#prefLabel', $object);

    my @res = ();
    while (my $st = $it->()) {
        push @res, $st->subject->uri;
    }

    return \@res;
}

sub flat(@) {
    return map { ref eq 'ARRAY' ? @$_ : $_ } @_;
}

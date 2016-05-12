#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;
use File::Temp qw(tempfile);

require Catmandu::Store::DBI;

my $driver_found = 1;
{
    local $@;
    eval {
        require DBD::SQLite;
    };
    if($@){
        $driver_found = 0;
    }
}


if(!$driver_found){

    plan skip_all => "database driver DBD::SQLite not found";

}else{

    my($fh,$file) = tempfile(UNLINK => 1,EXLOCK => 0);

    my $store = Catmandu::Store::DBI->new(
        data_source => "dbi:SQLite:dbname=$file",
        bags => {
            auto => { autocreate => 1 },
            non_auto => { autocreate => 0 }
        }
    );

    lives_ok(sub{

        $store->bag('auto')->add({ a => "a" });

    },"bag auto automatically created");

    dies_ok(sub{

        local $SIG{__WARN__} = sub { };

        $store->bag('non_auto')->add({ a => "a" });

    },"bag non_auto NOT automatically created");

    done_testing 2;

}

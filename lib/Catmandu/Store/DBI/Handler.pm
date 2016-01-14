package Catmandu::Store::DBI::Handler;

use Catmandu::Sane;
use Moo::Role;
use namespace::clean;

our $VERSION = "0.0503";

requires 'create_table';
requires 'add_row';
requires 'drop_database';
requires 'drop_table';

1;


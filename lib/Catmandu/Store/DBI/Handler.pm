package Catmandu::Store::DBI::Handler;

use Catmandu::Sane;
use Moo::Role;
use namespace::clean;

our $VERSION = "0.0503";

requires 'create_table';
requires 'add_row';
requires 'clear_database';
requires 'clear_table';

1;


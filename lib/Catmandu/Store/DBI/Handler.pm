package Catmandu::Store::DBI::Handler;

use Catmandu::Sane;
use Moo::Role;
use namespace::clean;

our $VERSION = "0.05";

requires 'create_table';
requires 'add_row';

1;


use Test::More tests => 5;

BEGIN {
use_ok( 'Parallel::Jobs' );
use_ok( 'Parallel::Jobs::Backend' );
use_ok( 'Parallel::Jobs::Backend::XMLRPC' );
use_ok( 'Parallel::Jobs::Backend::SSH' );
use_ok( 'Parallel::Rendezvous' );
}

diag( "Testing Parallel::Jobs $Parallel::Jobs::VERSION" );

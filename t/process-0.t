use Test::More tests => 9;

use_ok( 'Parallel::Jobs' );
use File::Basename;
use Data::Dumper;

my @hosts=(
  'host-1',
  'host-2',
  'host-3',
  'host-4',
  'host-5',
  'host-6',
  'host-7',
  'host-8',
  'host-9',
  'host-10',
  'host-11',
  'host-12',
  'host-13',
  'host-14',
  'host-15',
  'host-16',
  'host-17',
  'host-18',
  'host-19'
);
my $id;
#
#LOCAL JOB
###################################################################################
my $job=Parallel::Jobs->new(maxworkers=>5,timeout=>10, backend=>"Local");
ok(defined($job),"new local job");

$id=$job->create(hosts => \@hosts, command=>"`date`");
$info=$job->info();
ok($info->{$id}{'host-13'}{cmd} eq '`date`', "id=$id, host-13 command = `date` ");
ok(!defined ($info->{$id}{'host-13'}{pre}), "id=$id, host-13 pre is UNDEF");
ok(!defined ($info->{$id}{'host-13'}{post}), "id=$id, host-13 post is UNDEF");

@hosts=(
  'host-1',
);

$id=$job->create(hosts => \@hosts, 
                  pre=>"system",preparams=>("\"echo 'Olivier' >/tmp/perl-test-olivier\""), 
                  command=>"`cat /tmp/perl-test-olivier`", 
                  post=>"system", postparams=>"\"rm /tmp/perl-test-olivier\"");

$info=$job->info();

ok($info->{$id}{'host-1'}{pre}==0, "id=$id, host-1 post is 0");
ok($info->{$id}{'host-1'}{post}==0, "id=$id, host-1 post is 0");
ok($info->{$id}{'host-1'}{do} eq "Olivier\n", "id=$id, host-1 do eq Olivier");


#
#SSH JOB
###################################################################################
@hosts=(
  'localhost',
);

my $job=Parallel::Jobs->new(maxworkers=>5,timeout=>10, backend=>"SSH", constructor =>{user=>'demo',pass=>'demo'});


$id=$job->create(hosts => \@hosts, command=>"cat /proc/cmdline"); 

#$id=$job->create(hosts => \@hosts, 
#                  pre=>"system",preparams=>("\"echo 'Olivier' >perl-test-olivier\""), 
#                  command=>"cat /tmp/perl-test-olivier",); 
#                  post=>"system", postparams=>"\"rm perl-test-olivier\"");

$info=$job->info();
print Dumper $info;
ok($info->{$id}{'localhost'}{do} =~ /root=/, "id=$id, host-1 do /root=/");




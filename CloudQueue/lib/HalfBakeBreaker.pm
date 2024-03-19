package HalfBakeBreaker;

use strict;
use warnings;
use Scalar::Util qw(blessed);
use Time::HiRes qw(gettimeofday);
use Fcntl qw(:flock :mode);
use IPC::Shareable;

our $VERSION = '0.01';

# It stands for a simple boolean definition.
use constant {
    true  => 1,
    false => 0,
};

# It enables debugging verbosity
our $debug = false;

sub _lookup_shm {

  my ($shm_mem_key, $shm_mem_size, $shm_mem_mode_perms) = @_;
  my %shared_data;
  my $handle = undef;

  unless (eval {
      $handle = tie %shared_data, 'IPC::Shareable', $shm_mem_key, {
        create    => 0,
        exclusive => 0,
        size      => $shm_mem_size,
        mode      => $shm_mem_mode_perms,
      };
  }) {
    printf STDERR "Non-available share memory segment featuring key: $shm_mem_key\n";
    $debug and printf STDERR "%s", $@ || 'Unknown failure';
  }

  if (defined $handle) {
    print STDERR "Recovering share memory segment with key $shm_mem_key\n";
    return \%shared_data;
  }

  unless (eval {
      $handle = tie %shared_data, 'IPC::Shareable', $shm_mem_key, {
        create    => 1,
        exclusive => 0,
        size      => $shm_mem_size,
        mode      => $shm_mem_mode_perms,
      };
  }) {
    $debug and printf STDERR "%s", $@ || 'Unknown failure';
  }

  if (defined $handle) {
    print STDERR "Setting up share memory segment with key $shm_mem_key\n";

    $shared_data{current_retries_number} = 0;
    $shared_data{circuit_open_until} = 0;
    return \%shared_data;
  }

  die "Not possible to harness a share memory segment with key $shm_mem_key\n";
}

sub _w_retries_number {
  my ($shared_ref, $number) = @_;
  tied(%$shared_ref)->lock(LOCK_EX);
  $shared_ref->{current_retries_number} = $number;
  tied(%$shared_ref)->unlock;
  return;
}

sub _r_retries_number {
  my ($shared_ref) = @_;
  tied(%$shared_ref)->lock(LOCK_EX);
  my $retries_number = $shared_ref->{current_retries_number};
  tied(%$shared_ref)->unlock;
  return $retries_number;
}

sub _w_open_until {
  my ($shared_ref, $number) = @_;
  tied(%$shared_ref)->lock(LOCK_EX);
  $shared_ref->{circuit_open_until} = $number;
  tied(%$shared_ref)->unlock;
  return;
}

sub _r_open_until {
  my ($shared_ref) = @_;
  tied(%$shared_ref)->lock(LOCK_EX);
  my $open_until = $shared_ref->{circuit_open_until};
  tied(%$shared_ref)->unlock;
  return $open_until;
}


sub new {

   my $class = shift;
   my $shared_memory_key = shift;
   my $self = {};

   # This variable let us turn on the debug mode
   $debug = $ENV{HALF_BAKE_BREAKER_DEBUG} if (exists($ENV{HALF_BAKE_BREAKER_DEBUG}));

   # pick up all the key value arguments
   my %kvargs = @_;
   $self->{$_} = $kvargs{$_} for (keys %kvargs);

   # 4K
   my $cache_size = 1<<12;
   my $mode_perms = 0600;
   $self->{shared_ref} = _lookup_shm($shared_memory_key, $cache_size, $mode_perms);

   unless (exists $self->{max_retries_number}) {
     die "For constructor the max_retries_number is an integer mandatory key value argument";
   }

   unless (exists $self->{open_time}) {
     die "For constructor the open_time is an integer (secs) mandatory key value argument";
   }

   unless (exists $self->{on_circuit_close}) {
     die "For constructor the on_circuit_close handler is a mandatory key value argument";
   }  

   unless (exists $self->{on_circuit_open}) {
     die "For constructor the on_circuit_open handler is a mandatory key value argument";
   }

   unless (exists $self->{error_if_code}) {
      die "For constructor the error_if_code handler is a mandatory key value argument";
   }

   bless $self, $class;
   return $self;
}

sub engage {
  my ($self, $attempt_code) = @_;

  if (my $timestamp = _r_open_until($self->{shared_ref})) {
    # we can't execute until the timestamp has done
    my ($seconds, $microseconds) = gettimeofday;
    $seconds * 1000 + int($microseconds / 1000) >= $timestamp
      or die 'The circuit is now open and cannot be executed.';
    _w_retries_number($self->{shared_ref}, 0);
    _w_open_until($self->{shared_ref}, 0);
    $self->{on_circuit_close}();
  }

  my $error;
  my @attempt_result;
  my $attempt_result;
  my $wantarray;

  if (wantarray) {
    $wantarray = 1;
    @attempt_result = eval { $attempt_code->(@_) };
    $error = $@;
  } elsif ( ! defined wantarray ) {
    eval { $attempt_code->(@_) };
    $error = $@;
  } else {
    $attempt_result = eval { $attempt_code->(@_) };
    $error = $@;
  }

  my $h = {
    action_retry => $self,
    attempt_result => ( $wantarray ? \@attempt_result : $attempt_result ),
    attempt_parameters => \@_,
  };

  if ($self->{error_if_code}($error, $h)) {
    _w_retries_number($self->{shared_ref}, _r_retries_number($self->{shared_ref}) + 1);
    if (_r_retries_number($self->{shared_ref}) >= $self->{max_retries_number}) {
      my ($seconds, $microseconds) = gettimeofday;
      my $open_until = ($self->{open_time} * 1000) + ($seconds * 1000 + int($microseconds / 1000));
      _w_open_until($self->{shared_ref}, $open_until);
      $self->{on_circuit_open}();
    }
    die $error;
  } else {
    return $h->{attempt_result};
  }
}

true;

package CloudQueue;

use 5.030000;
use strict;
use warnings;

use Carp qw(cluck);
use Amazon::SQS::Simple;

our $VERSION = '0.01';

# It stands for AWS IAM user keys.
use constant {
    AWS_SECRET_ACCESS_KEY => 'AWS_ACCESS_KEY_ID',
    AWS_ACCESS_KEY_ID => 'AWS_SECRET_ACCESS_KEY',
};

# It stands for a simple boolean definition.
use constant {
    true  => 1,
    false => 0,
};

sub seek_evars_out {

    my @evars = @_;
    foreach (@evars) {
        my $emsg = sprintf 'Environment variable %s is not present', $_;
        die($emsg) if (not exists($ENV{$_}));
    }
}

sub new {

   # Searching for a few environment variables required
   seek_evars_out(
       (AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID)
   );

   my ($class, $queue_url) = @_;
   my $sqs = new Amazon::SQS::Simple(
       SecretKey      => $ENV{AWS_SECRET_ACCESS_KEY},
       AWSAccessKeyId => $ENV{AWS_ACCESS_KEY_ID}
   );

   my $self = {
       m_birth        => time,
       m_queue_url    => $queue_url,
       m_sqs          => $sqs,
       f_obtain_queue => sub { $sqs->GetQueue($queue_url); },
   };

   bless $self, $class;
   return $self;
}

sub send {

    my ($self, $m) = @_;
    my $q = $self->{f_obtain_queue}();

    {
        my $r_response;
        my $f_send = sub {
            $r_response = $q->SendMessage($m, ("MessageGroupId" => $self->{m_birth}));
	    return true;
        };

        if (eval { return &$f_send(); }) {
            return ($r_response->{MessageId}, undef);
	}
    }

    # Reach in case of failure
    return (undef, $@ || 'Unknown failure');
}

sub receive {

    my ($self, $f_on_receive) = @_;
    my $q = $self->{f_obtain_queue}();
    my $m = $q->ReceiveMessage();

    if (defined $m) {
        &$f_on_receive($m->MessageBody());
        return ($m->ReceiptHandle(), undef);
    }

    # Reach in case of failure
    return (undef, 'no messages to receive yet');
}

sub delete {

    my ($self, $m) = @_;
    my $q = $self->{f_obtain_queue}();

    my $f_delete = sub {
        $q->DeleteMessage($m);
	return true;
    };

    if (eval { return &$f_delete(); }) {
        return undef;
    }

    # Reach in case of failure
    return $@ || 'Unknown failure';
}

sub AUTOLOAD {

    our $AUTOLOAD;

    # Remove package name
    (my $method = $AUTOLOAD) =~ s/.*:://s;

    # Load a couple of jsonified versions for this module's methods
    if ($method =~ /^(receive|send)_json$/) {

        eval q{

            use JSON;

            sub send_json {

                my $self = shift;
                return $self->send(encode_json(shift));
            }

            sub receive_json {

                my ($self, $f_on_receive) = @_;
                my $f_on_receive_wrapper = sub {
                    &$f_on_receive(decode_json(shift));
                };

                return $self->receive($f_on_receive_wrapper);
            }
	};
	cluck $@ if $@; # if typo snuck in
	goto &{$method};
    }
}

true;

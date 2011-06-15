#
# Last example given at: http://freshmeat.net/articles/templates-in-ruby
#
# Here's example code that uses this class:

# require "Template"
# # Create the template object with the template and a hash
# temp = RevoTemplate.new( "Dear :::customer:::,\nPlease pay us :::amount::: now.\n" )
# temp.set( ":::", { 'customer' => 'Joe Schmoe', 'amount' => sprintf( "$%0.2f", 250 ) } )
# # Evaluate the template with the substitution values
# print temp


#
# Template: Implements a string substituation template.
# 
# The template should look like this:
#
# 'Dear :::customer:::,\nPlease pay us :::amount::: now.\n'
#
# With this template value, you could use the set method to set
# the values "customer" and "amount" with their appropriate replacement
# values.

# This version of the template class takes either a hash for values,
# or a method or function.  The function takes a single parameter,
# which is the key, and returns a single value, which is the string to
# be used as a replacement.


class RevoTemplate

  # Construct the template object with the template and the
  # replacement values.  "values" can be a hash, a function,
  # or a method.
  def initialize( template )
    @template = template.clone()
    @replaceStrs = {}
  end

  # Set up a replacement set.
  #
  # replaceStr: The string that starts and ends a replacement
  # item. For example, ":::" would mean that the replacement tokens
  # look like  :::name:::.
  #
  # values: The hash of values to replace the replacement token with,
  # or a method to call with a key.
  def set( replaceStr, values )
    if values.kind_of?( Hash )
      @replaceStrs[ replaceStr ] = values.method( :fetch )
    else
      @replaceStrs[ replaceStr ] = values.clone()
    end
  end

  # Run the template with the given parameters and return
  # the template with the values replaced
  def run()
    outStr = @template.clone()
    @replaceStrs.keys.each { |replaceStr|
      outStr.gsub!( /#{replaceStr}(.*?)#{replaceStr}/ ) {
          @replaceStrs[ replaceStr ].call( $1 ).to_s
      }
    }
    outStr
  end

  # A synonym for run so that you can simply print the class
  # and get the template result
  def to_s() 
    run()
  end
  
end

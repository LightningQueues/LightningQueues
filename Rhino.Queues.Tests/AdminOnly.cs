using System;
using System.Collections.Generic;
using System.Security.Principal;
using Xunit;
using Xunit.Sdk;

namespace Rhino.Queues.Tests
{
    public class AdminOnlyFactAttribute : FactAttribute
    {
        protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
        {
            if (IsUserAdministrator() == false)
            {
                return new[] { new SkipCommand(method, method.Name, "Cannot be run without admin permissions") };
            }
            return base.EnumerateTestCommands(method);
        }

        public bool IsUserAdministrator()
        {
            try
            {
                var user = WindowsIdentity.GetCurrent();
                if (user == null)
                    return false;
                var principal = new WindowsPrincipal(user);
                return principal.IsInRole(WindowsBuiltInRole.Administrator);
            }

            catch (Exception)
            {
                return false;
            }
        }
    }
}
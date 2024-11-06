# Compatibility tests

## The general idea

### Goals
Goals
The goal is to create a test battery based on the aforementioned framework, enabling all the parties involved with OpenLineage to test their components compatibility with both the OpenLineage standard and the other components. It should also be a guide for any aspiring OL supported on how to test said compatibility.

For any component integrating with OpenLineage there are 3 sides that are potentially interested in the test results:
- OpenLineage developers - contributing to spec 
- OpenLineage users
- Vendors - contributing to components

Each of the parties has different goals and expectations from the test suite.

### Producer tests requirements

<table style="width:100%;">
  <tr>
    <th style="width:33.33%; text-align:left; vertical-align:top;">Vendor</th>
    <th style="width:33.33%; text-align:left; vertical-align:top;">OL Dev</th>
    <th style="width:33.33%; text-align:left; vertical-align:top;">OL User</th>
  </tr>
  <tr>
    <td style="vertical-align:top;">
      - Test syntactically the events created by their newest version of the component.<br>
      - Verify if produced events are valid according to the spec.<br>
      - Identify deviations from the spec if events are not valid.<br>
      - Identify facets used by the producer that are not in the spec.<br>
      - Test the events semantically.
    </td>
    <td style="vertical-align:top;">
      - Check if the new version of the spec causes failures of producers.
    </td>
    <td style="vertical-align:top;">
      - Know which versions of the spec are supported by the producer.<br>
      - Overview of what is being tested, with at least a basic description of each test scenario.
    </td>
  </tr>
</table>


All of them want the tests to be easily runnable by any user in their own environment.

### Consumer test requirements

<table style="width:100%;">
  <tr>
    <th style="width:33.33%; text-align:left; vertical-align:top;">Vendor</th>
    <th style="width:33.33%; text-align:left; vertical-align:top;">OL Dev</th>
    <th style="width:33.33%; text-align:left; vertical-align:top;">OL User</th>
  </tr>
  <tr>
    <td style="vertical-align:top;">
      - Check which versions of the spec can be consumed without returning errors.<br>
      - Be notified when a change in the spec causes ingestion errors.<br>
      - Be notified when a new release of the consumer is not compatible with the spec.
    </td>
    <td style="vertical-align:top;">
      - See which consumers and producers are compliant with the current spec.<br>
      - See how changes made in the spec will affect consumer and producer compatibility.<br>
      - Maintain a list of components' maintainers.
    </td>
    <td style="vertical-align:top;">
      - Have an overview of what is being tested, with at least a basic description of each test scenario.<br>
      - Have information about the consumer's compatibility with the OL spec.<br>
      - Know which parts of OpenLineage are used by the consumer and how.<br>
      - Ensure this information is easily accessible.
    </td>
  </tr>
</table>


All of them want the tests to be easily runnable by any user in their own environment.

### Solution

The test suite addresses the goals with the following functionalities:

#### Have Clear Output

The test suite generates output in a structured JSON format for easy parsing and integration into other tools, while also providing a human-readable representation in markdown for accessibility and clarity.

#### Make As Much As Possible As Easy As Possible

The core features of the test suite are designed to be reusable and easy to access, which streamlines the testing process. Report generation is made plug-and-play, allowing users to quickly integrate reporting into their workflows. Test scenarios are structured for easy extensibility, enabling the addition of new tests with minimal effort. Test inputs for consumers are in common location, so each consumer has access to all of them. Furthermore, custom actions are defined in GitHub Actions to facilitate automation, including retrieving OpenLineage artifacts and validating events.

#### Locally Runnable

In addition to being integrated with GitHub Actions, the test suite can be run locally for each component using provided scripts, allowing developers to execute tests in their local environments easily.

#### Automated Testing and Notifications

The test suite automatically runs tests for each component, ensuring continuous verification of changes made to the codebase. When new failures are detected, notifications are automatically sent to the maintainers, promoting prompt issue resolution and accountability.

## Structure

### Producer

Each producer has its own directory, inside there are directories `runner` and `scenarios`. `runner` contains all the code necessary to create producer instance and run the tests. `scenarios` contains the test scenario definitions including code executed on the producer instance, expected output events and other files.

### Consumer

Consumer directory contains the common test events and list of consumer directories. Each consumer has a defined validator to check its state as well as scenario directories. Every scenario contains the expected API state after the scenario is executed and other files.


### Scripts

Scripts directory contains scripts used by handling the reports as well as scripts commonly used by the consumers or producers.

### Generated Files

Generated files directory contains the output files of the tests as well as files containing the state of the test suite e.g. list last checked versions of components or facets.

## Workflows

There are 3 workflows implemented in the repository:

- **Release Check Workflow**: Checks for new releases of OpenLineage or tested components, runs relevant tests, sends notifications in case of failures and updates the reports.

- **Spec Changes Check Workflow**: Checks main branch of OpenLineage repository for changes in the spec, runs relevant tests, sends notifications in case of failures and updates the reports.

- **PR Check Workflow**: Runs relevant tests on pull requests to ensure that changes do not cause any new failures for tested components.
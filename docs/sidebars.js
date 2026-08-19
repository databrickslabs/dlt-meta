/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      link: { type: 'doc', id: 'getting-started/index' },
      items: [
        'getting-started/quickstart',
        'getting-started/dabs',
        'getting-started/cli',
        'getting-started/manual',
        'getting-started/app',
        'getting-started/mcp',
        'getting-started/agent-skill',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      link: { type: 'doc', id: 'concepts/index' },
      items: [
        'concepts/architecture',
        'concepts/dataflowspec',
        'concepts/pipeline-chaining',
        'concepts/data-quality',
        {
          type: 'category',
          label: 'Governance',
          link: { type: 'doc', id: 'concepts/governance/index' },
          items: [
            'concepts/governance/catalog-scoped-tags',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      link: { type: 'doc', id: 'reference/index' },
      items: [
        'reference/onboarding-fields',
        'reference/silver-transformations',
        'reference/dq-rules',
        'reference/cli-commands',
        'reference/dab-parameters',
        {
          type: 'category',
          label: 'Governance',
          items: [
            'reference/governance/tags-yaml',
            'reference/governance/automation-manifests',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      link: { type: 'doc', id: 'guides/index' },
      items: [
        'guides/autoloader',
        'guides/kafka-eventhub',
        'guides/cdc',
        'guides/snapshot',
        'guides/silver-fanout',
        'guides/dlt-sink',
        'guides/multi-source-cdc',
        'guides/row-filters',
        {
          type: 'category',
          label: 'Governance',
          items: [
            'guides/governance/unity-catalog-tagging',
            'guides/governance/abac',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      link: { type: 'doc', id: 'operations/index' },
      items: [
        'operations/troubleshooting',
        'operations/integration-tests',
        'operations/migration',
        {
          type: 'category',
          label: 'Governance',
          items: [
            'operations/governance/prerequisites',
            'operations/governance/governed-tag-bootstrap',
          ],
        },
      ],
    },
    'contributing/index',
    'faq',
    'changelog',
  ],
};

module.exports = sidebars;

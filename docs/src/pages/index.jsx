import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';

const features = [
  {
    icon: '📋',
    title: 'Metadata-Driven',
    description:
      'Define your entire Bronze→Silver pipeline in a single onboarding JSON or YAML file. No pipeline code to write — SDP-META generates the Lakeflow graph for you.',
  },
  {
    icon: '📦',
    title: 'DAB-Native',
    description:
      'First-class Declarative Automation Bundle support. Git-tracked state, dev/prod promotion, and CI/CD-ready — scaffold a full bundle in one command.',
  },
  {
    icon: '✅',
    title: 'Built-in Data Quality',
    description:
      'Declare expectations inline. Records that fail go to a quarantine table automatically — no extra code, no extra pipelines.',
  },
  {
    icon: '🤖',
    title: 'AI-Ready via MCP',
    description:
      'Expose SDP-META operations as MCP tools so Claude Code, Claude Desktop, or Cursor can scaffold and inspect your pipelines on your behalf.',
  },
];

const quickstart = `# Install
pip install 'databricks-labs-sdp-meta[mcp]'
databricks labs install sdp-meta

# Scaffold a bundle (zero prompts, dev-friendly defaults)
databricks labs sdp-meta bundle-init --quickstart

# Validate and deploy
databricks labs sdp-meta bundle-validate
databricks bundle deploy`;

function HeroBanner() {
  return (
    <header
      style={{
        background: 'linear-gradient(135deg, #1B3139 0%, #2d5060 100%)',
        padding: '4rem 0 3rem',
        textAlign: 'center',
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      <div className="container">
        <div style={{ marginBottom: '1rem' }}>
          <span
            style={{
              background: '#FF3621',
              color: '#fff',
              padding: '0.25rem 0.75rem',
              borderRadius: '99px',
              fontSize: '0.8rem',
              fontWeight: 700,
              letterSpacing: '0.05em',
              textTransform: 'uppercase',
            }}
          >
            Databricks Labs
          </span>
        </div>
        <h1
          style={{
            fontSize: 'clamp(2rem, 5vw, 3.5rem)',
            fontWeight: 800,
            color: '#ffffff',
            marginBottom: '1rem',
          }}
        >
          SDP-META
        </h1>
        <p
          style={{
            fontSize: 'clamp(1rem, 2vw, 1.3rem)',
            color: 'rgba(255,255,255,0.85)',
            maxWidth: '640px',
            margin: '0 auto 2rem',
            lineHeight: 1.6,
          }}
        >
          Metadata-driven framework for automated{' '}
          <strong style={{ color: '#ffffff' }}>Bronze and Silver</strong> pipelines on{' '}
          <strong style={{ color: '#ffffff' }}>Lakeflow Spark Declarative Pipelines</strong>.
          Define your pipeline in JSON or YAML — SDP-META handles the rest.
        </p>
        <div style={{ display: 'flex', gap: '1rem', justifyContent: 'center', flexWrap: 'wrap' }}>
          <Link
            className="button button--lg"
            to="/docs/getting-started/quickstart"
            style={{ background: '#FF3621', border: 'none', color: '#fff', fontWeight: 700 }}
          >
            Get Started →
          </Link>
          <Link
            className="button button--lg button--outline"
            to="/docs/intro"
            style={{ color: '#fff', borderColor: 'rgba(255,255,255,0.5)' }}
          >
            What is SDP-META?
          </Link>
          <Link
            className="button button--lg button--outline"
            href="https://github.com/databrickslabs/sdp-meta"
            style={{ color: '#fff', borderColor: 'rgba(255,255,255,0.5)' }}
          >
            GitHub ↗
          </Link>
        </div>
        {/* Badges */}
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '0.5rem',
            justifyContent: 'center',
            marginTop: '2rem',
          }}
        >
          {[
            { label: 'docs', color: '#00A972', value: 'passing' },
            { label: 'pypi', color: '#0075c2', value: 'v0.1.0' },
            { label: 'license', color: '#555', value: 'Databricks Labs' },
          ].map((b) => (
            <span
              key={b.label}
              style={{
                display: 'inline-flex',
                fontSize: '0.75rem',
                borderRadius: '4px',
                overflow: 'hidden',
              }}
            >
              <span
                style={{
                  background: '#555',
                  color: '#fff',
                  padding: '0.2rem 0.5rem',
                }}
              >
                {b.label}
              </span>
              <span
                style={{
                  background: b.color,
                  color: '#fff',
                  padding: '0.2rem 0.5rem',
                  fontWeight: 600,
                }}
              >
                {b.value}
              </span>
            </span>
          ))}
        </div>
      </div>
    </header>
  );
}

function FeatureCard({ icon, title, description }) {
  return (
    <div className="col col--3" style={{ marginBottom: '1.5rem' }}>
      <div
        style={{
          padding: '1.5rem',
          borderRadius: '8px',
          border: '1px solid var(--ifm-color-emphasis-200)',
          height: '100%',
          transition: 'box-shadow 0.2s',
        }}
      >
        <div style={{ fontSize: '2.2rem', marginBottom: '0.75rem' }}>{icon}</div>
        <h3 style={{ fontWeight: 700, marginBottom: '0.5rem' }}>{title}</h3>
        <p style={{ fontSize: '0.9rem', color: 'var(--ifm-color-emphasis-700)', margin: 0 }}>
          {description}
        </p>
      </div>
    </div>
  );
}

function QuickstartSection() {
  return (
    <section style={{ padding: '3rem 0', background: 'var(--ifm-background-surface-color)' }}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <h2 style={{ fontWeight: 800, fontSize: '1.8rem' }}>Zero to pipeline in minutes</h2>
            <p style={{ color: 'var(--ifm-color-emphasis-700)', marginBottom: '1.5rem' }}>
              The recommended path is a <strong>Declarative Automation Bundle</strong> — git-tracked,
              CI/CD-ready, promotes across dev/prod. One command scaffolds everything.
            </p>
            <Link
              className="button button--primary button--lg"
              to="/docs/getting-started/quickstart"
              style={{ fontWeight: 700 }}
            >
              Full Quickstart Guide →
            </Link>
          </div>
          <div className="col col--6">
            <pre
              style={{
                background: '#1B3139',
                color: '#e2e8f0',
                borderRadius: '8px',
                padding: '1.25rem',
                fontSize: '0.82rem',
                lineHeight: 1.7,
                overflowX: 'auto',
                margin: 0,
              }}
            >
              <code>{quickstart}</code>
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
}

function PathsSection() {
  const paths = [
    {
      emoji: '📦',
      title: 'Declarative Automation Bundles',
      desc: 'Recommended. Git-tracked, CI/CD-ready, multi-target.',
      to: '/docs/getting-started/dabs',
      recommended: true,
    },
    {
      emoji: '⌨️',
      title: 'Interactive CLI',
      desc: 'Quick exploration. onboard + deploy in minutes.',
      to: '/docs/getting-started/cli',
      recommended: false,
    },
    {
      emoji: '🖥️',
      title: 'Lakehouse App',
      desc: 'GUI for non-engineers. Point-and-click onboarding.',
      to: '/docs/getting-started/app',
      recommended: false,
    },
    {
      emoji: '🤖',
      title: 'MCP Server',
      desc: 'AI-assisted. Let Claude scaffold your pipelines.',
      to: '/docs/getting-started/mcp',
      recommended: false,
    },
  ];

  return (
    <section style={{ padding: '3rem 0' }}>
      <div className="container">
        <h2 style={{ textAlign: 'center', fontWeight: 800, fontSize: '1.8rem', marginBottom: '0.5rem' }}>
          Choose your path
        </h2>
        <p style={{ textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem' }}>
          SDP-META supports four deployment interfaces. Start with DABs for any real work.
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
            gap: '1rem',
          }}
        >
          {paths.map((p) => (
            <Link
              key={p.title}
              to={p.to}
              style={{
                textDecoration: 'none',
                color: 'inherit',
                display: 'block',
                padding: '1.25rem',
                borderRadius: '8px',
                border: `2px solid ${p.recommended ? '#00A972' : 'var(--ifm-color-emphasis-200)'}`,
                transition: 'box-shadow 0.2s, border-color 0.2s',
              }}
            >
              {p.recommended && (
                <span
                  style={{
                    display: 'inline-block',
                    background: '#00A972',
                    color: '#fff',
                    fontSize: '0.65rem',
                    fontWeight: 700,
                    padding: '0.15rem 0.5rem',
                    borderRadius: '99px',
                    marginBottom: '0.5rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.06em',
                  }}
                >
                  Recommended
                </span>
              )}
              <div style={{ fontSize: '1.8rem', marginBottom: '0.4rem' }}>{p.emoji}</div>
              <div style={{ fontWeight: 700, marginBottom: '0.3rem' }}>{p.title}</div>
              <div style={{ fontSize: '0.85rem', color: 'var(--ifm-color-emphasis-700)' }}>{p.desc}</div>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Metadata-driven framework for automated Bronze and Silver pipelines on Lakeflow Spark Declarative Pipelines"
    >
      <HeroBanner />
      <main>
        <section style={{ padding: '3rem 0' }}>
          <div className="container">
            <div className="row">
              {features.map((f) => (
                <FeatureCard key={f.title} {...f} />
              ))}
            </div>
          </div>
        </section>
        <QuickstartSection />
        <PathsSection />
      </main>
    </Layout>
  );
}

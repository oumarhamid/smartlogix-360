import {
  useCallback,
  useEffect,
  useState,
  type FormEvent,
} from 'react'
import './App.css'

type ViewName = 'overview' | 'simulation' | 'optimization'

type TwinSummary = {
  total_orders: number
  at_risk_orders: number
  risk_rate: number
  observed_orders: number
  observation_rate: number
  evaluated_predictions: number
  average_delay_probability: number
  maximum_delay_probability: number
  full_history_orders: number
}

type CityTwinState = {
  city: string
  total_orders: number
  at_risk_orders: number
  risk_rate: number
  observed_orders: number
}

type TwinStateResponse = {
  summary: TwinSummary
  cities: CityTwinState[]
}

type SimulationResponse = {
  generated_at: string
  scenario: {
    name: string
    target_city: string | null
    demand_multiplier: number
    courier_capacity_multiplier: number
    sla_multiplier: number
    stress_strength: number
    pressure_factor: number
  }
  summary: {
    total_orders: number
    affected_orders: number
    baseline_at_risk: number
    simulated_at_risk: number
    at_risk_delta: number
    newly_at_risk: number
    recovered_from_risk: number
    baseline_risk_rate: number
    simulated_risk_rate: number
    baseline_average_probability: number
    simulated_average_probability: number
    average_probability_delta: number
  }
}

type OptimizationCandidate = {
  capacity_multiplier: number
  capacity_increase_rate: number
  pressure_factor: number
  affected_orders: number
  intervention_cost: number
  feasible: boolean
  target_met: boolean
  simulated_at_risk: number
  simulated_risk_rate: number
  simulated_average_probability: number
  at_risk_delta: number
  newly_at_risk: number
  recovered_from_risk: number
}

type OptimizationResponse = {
  generated_at: string
  baseline: {
    total_orders: number
    at_risk: number
    risk_rate: number
    average_probability: number
  }
  reference: OptimizationCandidate
  recommended: OptimizationCandidate
  decision: {
    target_met: boolean
    risk_reduction: number
    risk_rate_reduction: number
    risk_delta_vs_baseline: number
    risk_rate_delta_vs_baseline: number
  }
  candidates: OptimizationCandidate[]
}

const formatPercent = (value: number) =>
  `${(value * 100).toFixed(1)} %`

const formatMultiplier = (value: number) =>
  `× ${value.toFixed(2)}`

const formatNumber = (value: number) =>
  value.toLocaleString()

async function postJson<T>(
  url: string,
  body: unknown,
): Promise<T> {
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    const message = await response.text()

    throw new Error(
      `API HTTP ${response.status}${
        message ? ` — ${message}` : ''
      }`,
    )
  }

  return (await response.json()) as T
}

function App() {
  const [view, setView] = useState<ViewName>('overview')

  const [state, setState] =
    useState<TwinStateResponse | null>(null)

  const [loading, setLoading] = useState(true)
  const [error, setError] =
    useState<string | null>(null)

  const [updatedAt, setUpdatedAt] =
    useState<Date | null>(null)

  const [simulationLoading, setSimulationLoading] =
    useState(false)

  const [simulationError, setSimulationError] =
    useState<string | null>(null)

  const [simulationResult, setSimulationResult] =
    useState<SimulationResponse | null>(null)

  const [simulationDemand, setSimulationDemand] =
    useState(1.2)

  const [simulationCapacity, setSimulationCapacity] =
    useState(1.0)

  const [simulationSla, setSimulationSla] =
    useState(1.0)

  const [simulationStress, setSimulationStress] =
    useState(1.0)

  const [simulationCity, setSimulationCity] =
    useState('')

  const [optimizationLoading, setOptimizationLoading] =
    useState(false)

  const [optimizationError, setOptimizationError] =
    useState<string | null>(null)

  const [optimizationResult, setOptimizationResult] =
    useState<OptimizationResponse | null>(null)

  const [optimizationDemand, setOptimizationDemand] =
    useState(1.5)

  const [optimizationMinCapacity, setOptimizationMinCapacity] =
    useState(1.0)

  const [optimizationMaxCapacity, setOptimizationMaxCapacity] =
    useState(1.5)

  const [optimizationStep, setOptimizationStep] =
    useState(0.05)

  const [optimizationBudget, setOptimizationBudget] =
    useState(0.25)

  const [optimizationUnitCost, setOptimizationUnitCost] =
    useState(1.0)

  const [optimizationTargetRisk, setOptimizationTargetRisk] =
    useState(0.75)

  const [optimizationStress, setOptimizationStress] =
    useState(1.0)

  const [optimizationCity, setOptimizationCity] =
    useState('')

  const loadTwinState = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)

      const response = await fetch('/api/v1/twin/state')

      if (!response.ok) {
        throw new Error(`API HTTP ${response.status}`)
      }

      const payload =
        (await response.json()) as TwinStateResponse

      setState(payload)
      setUpdatedAt(new Date())
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : 'Impossible de charger le Digital Twin.',
      )
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadTwinState()
  }, [loadTwinState])

  const runSimulation = async (
    event: FormEvent<HTMLFormElement>,
  ) => {
    event.preventDefault()

    try {
      setSimulationLoading(true)
      setSimulationError(null)

      const result =
        await postJson<SimulationResponse>(
          '/api/v1/twin/simulations',
          {
            name: 'dashboard-what-if',
            demand_multiplier: simulationDemand,
            courier_capacity_multiplier:
              simulationCapacity,
            sla_multiplier: simulationSla,
            stress_strength: simulationStress,
            target_city:
              simulationCity === ''
                ? null
                : simulationCity,
          },
        )

      setSimulationResult(result)
    } catch (requestError) {
      setSimulationError(
        requestError instanceof Error
          ? requestError.message
          : 'La simulation a échoué.',
      )
    } finally {
      setSimulationLoading(false)
    }
  }

  const runOptimization = async (
    event: FormEvent<HTMLFormElement>,
  ) => {
    event.preventDefault()

    try {
      setOptimizationLoading(true)
      setOptimizationError(null)

      const result =
        await postJson<OptimizationResponse>(
          '/api/v1/twin/optimizations',
          {
            name: 'dashboard-capacity-optimization',
            demand_multiplier: optimizationDemand,
            capacity_min_multiplier:
              optimizationMinCapacity,
            capacity_max_multiplier:
              optimizationMaxCapacity,
            capacity_step: optimizationStep,
            budget: optimizationBudget,
            capacity_unit_cost:
              optimizationUnitCost,
            max_risk_rate:
              optimizationTargetRisk,
            stress_strength:
              optimizationStress,
            target_city:
              optimizationCity === ''
                ? null
                : optimizationCity,
          },
        )

      setOptimizationResult(result)
    } catch (requestError) {
      setOptimizationError(
        requestError instanceof Error
          ? requestError.message
          : "L'optimisation a échoué.",
      )
    } finally {
      setOptimizationLoading(false)
    }
  }

  const cityOptions = state?.cities ?? []

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div>
          <div className="brand-mark">SL</div>

          <h1>SmartLogix 360</h1>

          <p>
            Jumeau numérique logistique intelligent
          </p>

          <nav>
            <button
              className={`nav-item ${
                view === 'overview' ? 'active' : ''
              }`}
              onClick={() => setView('overview')}
            >
              Vue opérationnelle
            </button>

            <button
              className={`nav-item ${
                view === 'simulation' ? 'active' : ''
              }`}
              onClick={() => setView('simulation')}
            >
              Simulation
            </button>

            <button
              className={`nav-item ${
                view === 'optimization' ? 'active' : ''
              }`}
              onClick={() => setView('optimization')}
            >
              Optimisation
            </button>
          </nav>
        </div>

        <div className="sidebar-status">
          <span className="status-dot" />
          API jumeau numérique
        </div>
      </aside>

      <main className="main-content">
        {view === 'overview' && (
          <>
            <header className="page-header">
              <div>
                <span className="eyebrow">
                  JUMEAU NUMÉRIQUE / TEMPS RÉEL
                </span>

                <h2>Centre de contrôle logistique</h2>

                <p>
                  Supervision des opérations et du
                  risque de retard.
                </p>
              </div>

              <button
                className="refresh-button"
                onClick={() => void loadTwinState()}
                disabled={loading}
              >
                {loading
                  ? 'Actualisation...'
                  : 'Mise à jour'}
              </button>
            </header>

            {error && (
              <div className="error-banner">
                <strong>API indisponible.</strong>{' '}
                {error}
              </div>
            )}

            {!state && loading && (
              <div className="loading-panel">
                Chargement du jumeau numérique...
              </div>
            )}

            {state && (
              <>
                <section className="kpi-grid">
                  <article className="kpi-card">
                    <span>Commandes suivies</span>

                    <strong>
                      {formatNumber(
                        state.summary.total_orders,
                      )}
                    </strong>

                    <small>
                      Population du Digital Twin
                    </small>
                  </article>

                  <article className="kpi-card risk">
                    <span>Commandes à risque</span>

                    <strong>
                      {formatNumber(
                        state.summary.at_risk_orders,
                      )}
                    </strong>

                    <small>
                      {formatPercent(
                        state.summary.risk_rate,
                      )}{' '}
                      du total
                    </small>
                  </article>

                  <article className="kpi-card">
                    <span>Observations réelles</span>

                    <strong>
                      {formatNumber(
                        state.summary.observed_orders,
                      )}
                    </strong>

                    <small>
                      {formatPercent(
                        state.summary.observation_rate,
                      )}{' '}
                      observées
                    </small>
                  </article>

                  <article className="kpi-card">
                    <span>Historique complet J-1</span>

                    <strong>
                      {formatNumber(
                        state.summary.full_history_orders,
                      )}
                    </strong>

                    <small>
                      Commandes enrichies historiquement
                    </small>
                  </article>
                </section>

                <section className="content-card">
                  <div className="section-heading">
                    <div>
                      <span className="eyebrow">
                        ÉTAT OPÉRATIONNEL
                      </span>

                      <h3>
                        Jumeau numérique par ville
                      </h3>
                    </div>

                    <span className="city-count">
                      {state.cities.length} villes
                    </span>
                  </div>

                  <div className="table-wrapper">
                    <table>
                      <thead>
                        <tr>
                          <th>Ville</th>
                          <th>Commandes</th>
                          <th>À risque</th>
                          <th>Taux de risque</th>
                          <th>Observées</th>
                        </tr>
                      </thead>

                      <tbody>
                        {state.cities.map((city) => (
                          <tr key={city.city}>
                            <td className="city-name">
                              {city.city}
                            </td>

                            <td>
                              {formatNumber(
                                city.total_orders,
                              )}
                            </td>

                            <td>
                              {formatNumber(
                                city.at_risk_orders,
                              )}
                            </td>

                            <td>
                              <span
                                className={`risk-badge ${
                                  city.risk_rate >= 0.5
                                    ? 'high'
                                    : 'normal'
                                }`}
                              >
                                {formatPercent(
                                  city.risk_rate,
                                )}
                              </span>
                            </td>

                            <td>
                              {formatNumber(
                                city.observed_orders,
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </section>

                <section className="insight-grid">
                  <article className="content-card">
                    <span className="eyebrow">
                      PRÉDICTION
                    </span>

                    <h3>
                      Probabilité moyenne de retard
                    </h3>

                    <div className="metric-large">
                      {formatPercent(
                        state.summary
                          .average_delay_probability,
                      )}
                    </div>
                  </article>

                  <article className="content-card">
                    <span className="eyebrow">
                      RISQUE MAXIMUM
                    </span>

                    <h3>
                      Probabilité maximale observée
                    </h3>

                    <div className="metric-large risk-text">
                      {formatPercent(
                        state.summary
                          .maximum_delay_probability,
                      )}
                    </div>
                  </article>

                  <article className="content-card">
                    <span className="eyebrow">
                      ÉVALUATION LIVE
                    </span>

                    <h3>Prédictions évaluées</h3>

                    <div className="metric-large">
                      {
                        state.summary
                          .evaluated_predictions
                      }
                    </div>
                  </article>
                </section>
              </>
            )}
          </>
        )}

        {view === 'simulation' && (
          <>
            <header className="page-header">
              <div>
                <span className="eyebrow">
                  DIGITAL TWIN / WHAT-IF
                </span>

                <h2>Simulation opérationnelle</h2>

                <p>
                  Mesurer l'impact d'un changement de
                  demande, capacité ou SLA.
                </p>
              </div>
            </header>

            <section className="workspace-grid">
              <form
                className="content-card control-panel"
                onSubmit={runSimulation}
              >
                <div className="section-heading">
                  <div>
                    <span className="eyebrow">
                      SCÉNARIO
                    </span>

                    <h3>Paramètres What-If</h3>
                  </div>
                </div>

                <div className="form-grid">
                  <label>
                    Multiplicateur de demande
                    <input
                      type="number"
                      min="0.05"
                      step="0.05"
                      value={simulationDemand}
                      onChange={(event) =>
                        setSimulationDemand(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Capacité coursiers
                    <input
                      type="number"
                      min="0.05"
                      step="0.05"
                      value={simulationCapacity}
                      onChange={(event) =>
                        setSimulationCapacity(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Multiplicateur SLA
                    <input
                      type="number"
                      min="0.05"
                      step="0.05"
                      value={simulationSla}
                      onChange={(event) =>
                        setSimulationSla(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Intensité du stress
                    <input
                      type="number"
                      min="0"
                      step="0.1"
                      value={simulationStress}
                      onChange={(event) =>
                        setSimulationStress(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label className="full-field">
                    Ville ciblée
                    <select
                      value={simulationCity}
                      onChange={(event) =>
                        setSimulationCity(
                          event.target.value,
                        )
                      }
                    >
                      <option value="">
                        Toutes les villes
                      </option>

                      {cityOptions.map((city) => (
                        <option
                          key={city.city}
                          value={city.city}
                        >
                          {city.city}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>

                <button
                  className="primary-button"
                  type="submit"
                  disabled={simulationLoading}
                >
                  {simulationLoading
                    ? 'Simulation...'
                    : 'Lancer la simulation'}
                </button>

                {simulationError && (
                  <div className="error-banner compact">
                    {simulationError}
                  </div>
                )}
              </form>

              <section className="content-card results-panel">
                <span className="eyebrow">
                  RÉSULTATS
                </span>

                <h3>Impact du scénario</h3>

                {!simulationResult && (
                  <div className="empty-state">
                    Lance un scénario pour comparer le
                    risque simulé à la situation actuelle.
                  </div>
                )}

                {simulationResult && (
                  <>
                    <div className="result-kpis">
                      <div>
                        <span>Risque actuel</span>
                        <strong>
                          {formatPercent(
                            simulationResult.summary
                              .baseline_risk_rate,
                          )}
                        </strong>
                      </div>

                      <div>
                        <span>Risque simulé</span>
                        <strong
                          className={
                            simulationResult.summary
                              .at_risk_delta > 0
                              ? 'negative'
                              : 'positive'
                          }
                        >
                          {formatPercent(
                            simulationResult.summary
                              .simulated_risk_rate,
                          )}
                        </strong>
                      </div>

                      <div>
                        <span>Commandes à risque</span>
                        <strong>
                          {
                            simulationResult.summary
                              .simulated_at_risk
                          }
                        </strong>
                      </div>

                      <div>
                        <span>Variation</span>
                        <strong
                          className={
                            simulationResult.summary
                              .at_risk_delta > 0
                              ? 'negative'
                              : 'positive'
                          }
                        >
                          {simulationResult.summary
                            .at_risk_delta > 0
                            ? '+'
                            : ''}
                          {
                            simulationResult.summary
                              .at_risk_delta
                          }
                        </strong>
                      </div>
                    </div>

                    <div className="scenario-summary">
                      <div>
                        <span>Pression</span>
                        <strong>
                          {simulationResult.scenario
                            .pressure_factor.toFixed(2)}
                        </strong>
                      </div>

                      <div>
                        <span>Commandes affectées</span>
                        <strong>
                          {
                            simulationResult.summary
                              .affected_orders
                          }
                        </strong>
                      </div>

                      <div>
                        <span>Probabilité moyenne</span>
                        <strong>
                          {formatPercent(
                            simulationResult.summary
                              .simulated_average_probability,
                          )}
                        </strong>
                      </div>
                    </div>
                  </>
                )}
              </section>
            </section>
          </>
        )}

        {view === 'optimization' && (
          <>
            <header className="page-header">
              <div>
                <span className="eyebrow">
                  DIGITAL TWIN / PRESCRIPTIF
                </span>

                <h2>Optimisation de capacité</h2>

                <p>
                  Rechercher automatiquement une
                  intervention sous contrainte de budget.
                </p>
              </div>
            </header>

            <section className="workspace-grid">
              <form
                className="content-card control-panel"
                onSubmit={runOptimization}
              >
                <div className="section-heading">
                  <div>
                    <span className="eyebrow">
                      PROBLÈME
                    </span>

                    <h3>Contraintes d'optimisation</h3>
                  </div>
                </div>

                <div className="form-grid">
                  <label>
                    Demande
                    <input
                      type="number"
                      min="0.05"
                      step="0.05"
                      value={optimizationDemand}
                      onChange={(event) =>
                        setOptimizationDemand(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Capacité minimale
                    <input
                      type="number"
                      min="1"
                      step="0.05"
                      value={optimizationMinCapacity}
                      onChange={(event) =>
                        setOptimizationMinCapacity(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Capacité maximale
                    <input
                      type="number"
                      min="1"
                      step="0.05"
                      value={optimizationMaxCapacity}
                      onChange={(event) =>
                        setOptimizationMaxCapacity(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Pas de recherche
                    <input
                      type="number"
                      min="0.01"
                      step="0.01"
                      value={optimizationStep}
                      onChange={(event) =>
                        setOptimizationStep(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Budget
                    <input
                      type="number"
                      min="0"
                      step="0.05"
                      value={optimizationBudget}
                      onChange={(event) =>
                        setOptimizationBudget(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Coût unitaire
                    <input
                      type="number"
                      min="0.01"
                      step="any"
                      value={optimizationUnitCost}
                      onChange={(event) =>
                        setOptimizationUnitCost(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Risque cible
                    <input
                      type="number"
                      min="0"
                      max="1"
                      step="0.01"
                      value={optimizationTargetRisk}
                      onChange={(event) =>
                        setOptimizationTargetRisk(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label>
                    Intensité du stress
                    <input
                      type="number"
                      min="0"
                      step="0.1"
                      value={optimizationStress}
                      onChange={(event) =>
                        setOptimizationStress(
                          Number(event.target.value),
                        )
                      }
                    />
                  </label>

                  <label className="full-field">
                    Ville ciblée
                    <select
                      value={optimizationCity}
                      onChange={(event) =>
                        setOptimizationCity(
                          event.target.value,
                        )
                      }
                    >
                      <option value="">
                        Toutes les villes
                      </option>

                      {cityOptions.map((city) => (
                        <option
                          key={city.city}
                          value={city.city}
                        >
                          {city.city}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>

                <button
                  className="primary-button"
                  type="submit"
                  disabled={optimizationLoading}
                >
                  {optimizationLoading
                    ? 'Optimisation...'
                    : 'Calculer la recommandation'}
                </button>

                {optimizationError && (
                  <div className="error-banner compact">
                    {optimizationError}
                  </div>
                )}
              </form>

              <section className="content-card results-panel">
                <span className="eyebrow">
                  RECOMMANDATION
                </span>

                <h3>Décision prescriptive</h3>

                {!optimizationResult && (
                  <div className="empty-state">
                    Lance l'optimisation pour obtenir une
                    recommandation de capacité.
                  </div>
                )}

                {optimizationResult && (
                  <>
                    <div
                      className={`decision-banner ${
                        optimizationResult.decision
                          .target_met
                          ? 'success'
                          : 'warning'
                      }`}
                    >
                      <strong>
                        {optimizationResult.decision
                          .target_met
                          ? 'Objectif atteint'
                          : 'Objectif non atteint'}
                      </strong>

                      <span>
                        Capacité recommandée :{' '}
                        {formatMultiplier(
                          optimizationResult.recommended
                            .capacity_multiplier,
                        )}
                      </span>
                    </div>

                    <div className="result-kpis">
                      <div>
                        <span>Risque sous stress</span>
                        <strong>
                          {
                            optimizationResult.reference
                              .simulated_at_risk
                          }
                        </strong>
                      </div>

                      <div>
                        <span>Après intervention</span>
                        <strong>
                          {
                            optimizationResult.recommended
                              .simulated_at_risk
                          }
                        </strong>
                      </div>

                      <div>
                        <span>Risques évités</span>
                        <strong className="positive">
                          {
                            optimizationResult.decision
                              .risk_reduction
                          }
                        </strong>
                      </div>

                      <div>
                        <span>Coût</span>
                        <strong>
                          {optimizationResult.recommended
                            .intervention_cost.toFixed(2)}
                        </strong>
                      </div>
                    </div>

                    <div className="scenario-summary">
                      <div>
                        <span>Hausse de capacité</span>
                        <strong>
                          {formatPercent(
                            optimizationResult.recommended
                              .capacity_increase_rate,
                          )}
                        </strong>
                      </div>

                      <div>
                        <span>Risque final</span>
                        <strong>
                          {formatPercent(
                            optimizationResult.recommended
                              .simulated_risk_rate,
                          )}
                        </strong>
                      </div>

                      <div>
                        <span>
                          Écart vs baseline
                        </span>
                        <strong
                          className={
                            optimizationResult.decision
                              .risk_delta_vs_baseline > 0
                              ? 'negative'
                              : 'positive'
                          }
                        >
                          {optimizationResult.decision
                            .risk_delta_vs_baseline > 0
                            ? '+'
                            : ''}
                          {
                            optimizationResult.decision
                              .risk_delta_vs_baseline
                          }
                        </strong>
                      </div>
                    </div>
                  </>
                )}
              </section>
            </section>

            {optimizationResult && (
              <section className="content-card candidate-card">
                <div className="section-heading">
                  <div>
                    <span className="eyebrow">
                      ESPACE DE RECHERCHE
                    </span>

                    <h3>Solutions candidates</h3>
                  </div>

                  <span className="city-count">
                    {
                      optimizationResult.candidates
                        .length
                    }{' '}
                    scénarios
                  </span>
                </div>

                <div className="table-wrapper">
                  <table>
                    <thead>
                      <tr>
                        <th>Capacité</th>
                        <th>Coût</th>
                        <th>Pression</th>
                        <th>À risque</th>
                        <th>Taux risque</th>
                        <th>Faisable</th>
                      </tr>
                    </thead>

                    <tbody>
                      {optimizationResult.candidates.map(
                        (candidate) => (
                          <tr
                            key={
                              candidate.capacity_multiplier
                            }
                          >
                            <td className="city-name">
                              {formatMultiplier(
                                candidate.capacity_multiplier,
                              )}
                            </td>

                            <td>
                              {candidate.intervention_cost.toFixed(
                                2,
                              )}
                            </td>

                            <td>
                              {candidate.pressure_factor.toFixed(
                                2,
                              )}
                            </td>

                            <td>
                              {
                                candidate.simulated_at_risk
                              }
                            </td>

                            <td>
                              {formatPercent(
                                candidate.simulated_risk_rate,
                              )}
                            </td>

                            <td>
                              <span
                                className={`status-badge ${
                                  candidate.feasible
                                    ? 'success'
                                    : 'warning'
                                }`}
                              >
                                {candidate.feasible
                                  ? 'Oui'
                                  : 'Non'}
                              </span>
                            </td>
                          </tr>
                        ),
                      )}
                    </tbody>
                  </table>
                </div>
              </section>
            )}
          </>
        )}

        <footer>
          {updatedAt
            ? `Dernière actualisation : ${updatedAt.toLocaleTimeString()}`
            : 'SmartLogix 360'}
        </footer>
      </main>
    </div>
  )
}

export default App
